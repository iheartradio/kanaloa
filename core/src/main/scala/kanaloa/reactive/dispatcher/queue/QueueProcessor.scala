package kanaloa.reactive.dispatcher.queue



import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import kanaloa.reactive.dispatcher.ApiProtocol._
import kanaloa.reactive.dispatcher.metrics.{Metric, Reporter, MetricsCollector}
import kanaloa.reactive.dispatcher.queue.Queue.Retire
import kanaloa.reactive.dispatcher.queue.QueueProcessor._
import kanaloa.reactive.dispatcher.queue.Worker.Hold
import kanaloa.reactive.dispatcher.{Backend, ResultChecker}
import kanaloa.util.FiniteCollection._
import kanaloa.util.MessageScheduler

import util.{Failure, Success}
import scala.concurrent.duration._

trait QueueProcessor extends Actor with ActorLogging with MessageScheduler {
  import QueueProcessor.WorkerPool

  val queue: QueueRef
  def backend: Backend
  def settings: ProcessingWorkerPoolSettings
  def resultChecker: ResultChecker
  val metricsCollector: ActorRef
  def workerFactory : WorkerFactory
  type ResultHistory = Vector[Boolean]
  val resultHistoryLength: Int
  var workerCount = 0
  protected def onWorkError(resultHistory: ResultHistory, pool: WorkerPool)
  var resultHistory = Vector[Boolean]()
  var workerPool = Set[ActorRef]()

  metricsCollector ! Metric.PoolSize(settings.startingPoolSize)

  //stop any children which failed.  Let the DeathWatch handle it
  override val supervisorStrategy = SupervisorStrategy.stoppingStrategy

  override def preStart(): Unit = {
    super.preStart()
    (1 to settings.startingPoolSize).foreach(_ ⇒ createRoutee())
    settings.maxProcessingTime.foreach(delayedMsg(_, QueueMaxProcessTimeReached(queue)))
    context watch queue
  }

  def recordWorkError(): Unit = {
    this.resultHistory = resultHistory.enqueueFinite(false, resultHistoryLength)
    onWorkError(resultHistory, workerPool)
  }

  def receive: Receive = {
    case ScaleTo(newPoolSize, reason) ⇒
      log.debug(s"Command to scale to $newPoolSize, currently at ${workerPool.size} due to ${reason.getOrElse("no reason given")}")
      val toPoolSize = Math.max(settings.minPoolSize, Math.min(settings.maxPoolSize, newPoolSize))
      val diff = toPoolSize - workerPool.size
      if (diff > 0) {
        metricsCollector ! Metric.PoolSize(newPoolSize)
        (1 to diff).foreach(_ =>createRoutee())
      }
      else if (diff < 0)
        workerPool.take(-diff).foreach(_ ! Worker.Retire)

    case RouteeCreated(routee) ⇒
      workerPool = workerPool + createWorker(routee)

    case RouteeFailed ⇒ //?

      case WorkCompleted(worker, duration) ⇒
        resultHistory = resultHistory.enqueueFinite(true, resultHistoryLength)
        metricsCollector ! Metric.WorkCompleted(duration)

    case WorkFailed(_) ⇒
      recordWorkError()
      metricsCollector ! Metric.WorkFailed

    case WorkTimedOut(_) ⇒
      recordWorkError()
      metricsCollector ! Metric.WorkTimedOut

    case Terminated(worker) if workerPool.contains(worker) ⇒
      removeWorker(worker)

    //if workers drop below minimum, we have a problem.

    //if the Queue terminated, time to shut stuff down.
    case Terminated(`queue`) ⇒
      log.debug(s"Queue ${queue.path} is terminated")
      if (workerPool.isEmpty) {
        context stop self
      } else {
        workerPool.foreach(_ ! Worker.Retire)
        delayedMsg(3.minutes, ShutdownTimeout) //TODO: hardcoded
        context become shuttingDown(None)
      }

    case qs: QueryStatus ⇒ qs reply RunningStatus(workerPool)

    //queue processor initiates shutdown of everyone.
    case Shutdown(reportTo, timeout) ⇒
      log.info("Commanded to shutdown. Shutting down")
      queue ! Retire(timeout)
      delayedMsg(timeout, ShutdownTimeout)
      context become shuttingDown(reportTo)
  }

  def shuttingDown(reportTo: Option[ActorRef]): Receive = {

    case Terminated(worker) if workerPool.contains(worker) ⇒
      removeWorker(worker)
      if (workerPool.isEmpty) {
        log.info(s"All Workers have terminated, QueueProcessor is shutting down")
        reportTo.foreach(_ ! ShutdownSuccessfully)
        context stop self
      }

    case Terminated(_)   ⇒ //ignore other termination

    case qs: QueryStatus ⇒ qs reply ShuttingDown

    case ShutdownTimeout ⇒
      log.warning("Shutdown timed out, forcefully shutting down")
      workerPool.foreach(_ ! PoisonPill)
      context stop self

    case _ ⇒ //Ignore
  }

  def removeWorker(worker: ActorRef): Unit = {
    context.unwatch(worker)
    workerPool = workerPool - worker
    metricsCollector ! Metric.PoolSize(workerPool.size)
  }

  private def createRoutee(): Unit = {
    import context.dispatcher //do we want to pass this in?
    backend(this.context).onComplete {
      case Success(routee) ⇒ self ! RouteeCreated(routee)
      case Failure(ex) ⇒ {
        log.error(ex, s"could not create new Routee")
        self ! RouteeFailed
      }
    }
  }

  private def createWorker(routee: ActorRef): WorkerRef = {
    val workerName = s"worker-$workerCount"
    workerCount += 1
    val worker = workerFactory.createWorker(queue, routee, resultChecker, workerName)
    context watch worker
    worker
  }
}

/**
 * The default queue processor uses the same [[ ResultChecker ]] for all queues
 *
 * @param resultChecker
 */
case class DefaultQueueProcessor(
  queue:            QueueRef,
  backend:          Backend,
  settings:         ProcessingWorkerPoolSettings,
  metricsCollector: ActorRef,
  workerFactory:    WorkerFactory,
  resultChecker:    ResultChecker
) extends QueueProcessor {

  override val resultHistoryLength: Int = 0

  override protected def onWorkError(resultHistory: ResultHistory, pool: WorkerPool): Unit = () //do nothing
}

case class QueueProcessorWithCircuitBreaker(
  queue:                  QueueRef,
  backend:                Backend,
  settings:               ProcessingWorkerPoolSettings,
  circuitBreakerSettings: CircuitBreakerSettings,
  metricsCollector: ActorRef,
  workerFactory:    WorkerFactory,
  resultChecker:          ResultChecker
) extends QueueProcessor {

  override val resultHistoryLength: Int = circuitBreakerSettings.historyLength

  override protected def onWorkError(resultHistory: ResultHistory, pool: WorkerPool): Unit = {
    if ((resultHistory.count(r ⇒ !r).toDouble / resultHistoryLength) >= circuitBreakerSettings.errorRateThreshold) {
      metricsCollector ! Metric.CircuitBreakerOpened
      pool.foreach(_ ! Hold(circuitBreakerSettings.closeDuration))
    }
  }
}

trait WorkerFactory {
  def createWorker(queueRef: ActorRef, routee: ActorRef, resultChecker: ResultChecker, workerName: String)(implicit ac: ActorRefFactory): ActorRef
}

object DefaultWorkerFactory extends WorkerFactory {

  override def createWorker(queue: QueueRef, routee: QueueRef, resultChecker: ResultChecker, workerName: String)(implicit ac: ActorRefFactory): ActorRef = {
    ac.actorOf(Worker.default(queue, routee)(resultChecker), workerName)
  }
}

object QueueProcessor {
  private[queue]type WorkerPool = Set[WorkerRef]

  case class ScaleTo(numOfWorkers: Int, reason: Option[String] = None) {
    assert(numOfWorkers >= 0)
  }

  case class WorkCompleted(worker: WorkerRef, duration: FiniteDuration)

  case class QueueMaxProcessTimeReached(queue: QueueRef)
  case class RunningStatus(pool: WorkerPool)
  case object ShuttingDown
  private[queue] case class RouteeCreated(routee: ActorRef)
  private[queue] case object RouteeFailed
  case class Shutdown(reportBackTo: Option[ActorRef] = None, timeout: FiniteDuration = 3.minutes)
  private case object ShutdownTimeout

  def default(
    queue:            QueueRef,
    backend:          Backend,
    settings:         ProcessingWorkerPoolSettings,
    metricsCollector: ActorRef,
    workerFactory:    WorkerFactory                = DefaultWorkerFactory
  )(resultChecker: ResultChecker): Props =
    Props(new DefaultQueueProcessor(
      queue,
      backend,
      settings,
      metricsCollector,
      workerFactory,
      resultChecker
    )).withDeploy(Deploy.local)

  def withCircuitBreaker(
    queue:                  QueueRef,
    backend:                Backend,
    settings:               ProcessingWorkerPoolSettings,
    circuitBreakerSettings: CircuitBreakerSettings,
    metricsCollector: ActorRef,
    workerFactory:    WorkerFactory                = DefaultWorkerFactory
                        )(resultChecker: ResultChecker): Props =
    Props(new QueueProcessorWithCircuitBreaker(
      queue,
      backend,
      settings,
      circuitBreakerSettings,
      metricsCollector,
      workerFactory,
      resultChecker
    )).withDeploy(Deploy.local)
}

