package kanaloa.reactive.dispatcher.queue

import akka.actor._
import kanaloa.reactive.dispatcher.ApiProtocol._
import kanaloa.reactive.dispatcher.metrics.Metric
import kanaloa.reactive.dispatcher.queue.Queue.Retire
import kanaloa.reactive.dispatcher.queue.QueueProcessor._
import kanaloa.reactive.dispatcher.{Backend, ResultChecker}
import kanaloa.util.MessageScheduler

import scala.concurrent.duration._
import scala.util.{Failure, Success}

class QueueProcessor(
  queue:                  QueueRef,
  backend:                Backend,
  settings:               ProcessingWorkerPoolSettings,
  circuitBreakerSettings: Option[CircuitBreakerSettings],
  metricsCollector:       ActorRef,
  workerFactory:          WorkerFactory,
  resultChecker:          ResultChecker
) extends Actor with ActorLogging with MessageScheduler {

  val healthCheckSchedule = {
    import context.dispatcher
    context.system.scheduler.schedule(settings.healthCheckInterval, settings.healthCheckInterval, self, HealthCheck)
  }

  var workerCount = 0
  var workerPool: WorkerPool = List[ActorRef]()
  var inflightCreations = 0

  //stop any children which failed.  Let the DeathWatch handle it
  override val supervisorStrategy = SupervisorStrategy.stoppingStrategy

  override def preStart(): Unit = {
    super.preStart()
    (1 to settings.startingPoolSize).foreach(_ ⇒ retrieveRoutee())
    context watch queue
  }

  def currentWorkers = workerPool.size + inflightCreations

  def receive: Receive = {
    case ScaleTo(newPoolSize, reason) ⇒
      log.debug(s"Command to scale to $newPoolSize, currently at ${workerPool.size} due to ${reason.getOrElse("no reason given")}")
      val toPoolSize = Math.max(settings.minPoolSize, Math.min(settings.maxPoolSize, newPoolSize))
      val diff = toPoolSize - currentWorkers

      tryCreateWorkersIfNeeded(diff)
      if (diff < 0) {
        workerPool.take(-diff).foreach(_ ! Worker.Retire)
      }

    case RouteeRetrieved(routee) ⇒
      createWorker(routee)
      metricsCollector ! Metric.PoolSize(workerPool.size)

    case RouteeFailed(ex) ⇒
      inflightCreations -= 1
      if (settings.logRouteeRetrievalError)
        log.warning("Failed to retrieve Routee: " + ex.getMessage)

    case Terminated(worker) if workerPool.contains(worker) ⇒
      removeWorker(worker)
      healthCheck()

    case HealthCheck ⇒ healthCheck()

    //if the Queue terminated, time to shut stuff down.
    case Terminated(`queue`) ⇒
      log.debug(s"Queue ${queue.path} is terminated")
      healthCheckSchedule.cancel()
      if (workerPool.isEmpty) {
        context stop self
      } else {
        workerPool.foreach(_ ! Worker.Retire)
        delayedMsg(3.minutes, ShutdownTimeout) //TODO: hardcoded it's a forced shutdown
        context become shuttingDown(None)
      }

    case qs: QueryStatus ⇒ qs reply RunningStatus(workerPool)

    //queue processor initiates shutdown of everyone.
    case Shutdown(reportTo, timeout) ⇒
      log.info("Commanded to shutdown. Shutting down")
      queue ! Retire(timeout)
      delayedMsg(timeout, ShutdownTimeout)
      healthCheckSchedule.cancel()
      context become shuttingDown(reportTo)
  }

  def shuttingDown(reportTo: Option[ActorRef]): Receive = {
    def tryFinish(): Unit = {
      if (workerPool.isEmpty) {
        log.info(s"All Workers have terminated, QueueProcessor is shutting down")
        reportTo.foreach(_ ! ShutdownSuccessfully)
        context stop self
      }
    }
    {
      case Terminated(worker) if workerPool.contains(worker) ⇒
        removeWorker(worker)
        tryFinish()

      case Terminated(`queue`) ⇒
        tryFinish()

      case qs: QueryStatus ⇒ qs reply ShuttingDown

      case ShutdownTimeout ⇒
        log.warning("Shutdown timed out, forcefully shutting down")
        reportTo.foreach(_ ! ShutdownForcefully)
        context stop self

      case m ⇒ log.info("message received and ignored during shutdown: " + m)

    }: Receive
  }

  private def removeWorker(worker: ActorRef): Unit = {
    context.unwatch(worker)
    workerPool = workerPool.filter(_ != worker)
    metricsCollector ! Metric.PoolSize(workerPool.size)
  }

  private def retrieveRoutee(): Unit = {
    import context.dispatcher //do we want to pass this in?
    inflightCreations += 1
    backend(context).onComplete {
      case Success(routee) ⇒ self ! RouteeRetrieved(routee)
      case Failure(ex)     ⇒ self ! RouteeFailed(ex)
    }
  }

  private def healthCheck(): Unit = {
    if (tryCreateWorkersIfNeeded(settings.minPoolSize - currentWorkers))
      log.warning("Number of workers in pool is below minimum. Trying to replenish. ")
  }
  /**
   *
   * @param numberOfWorkersToCreate
   * @return true if workers are scheduled to be created
   */
  private def tryCreateWorkersIfNeeded(numberOfWorkersToCreate: Int): Boolean = {
    val workerNeeded = numberOfWorkersToCreate > 0
    if (workerNeeded)
      (1 to numberOfWorkersToCreate).foreach(_ ⇒ retrieveRoutee())
    workerNeeded
  }

  private def createWorker(routee: ActorRef): Unit = {
    val workerName = s"worker-$workerCount"
    workerCount += 1
    val worker = workerFactory.createWorker(queue, routee, metricsCollector, circuitBreakerSettings, resultChecker, workerName)
    context watch worker

    workerPool = workerPool :+ worker
    inflightCreations -= 1
  }
}

private[queue] trait WorkerFactory {
  def createWorker(
    queueRef:               ActorRef,
    routee:                 ActorRef,
    metricsCollector:       ActorRef,
    circuitBreakerSettings: Option[CircuitBreakerSettings],
    resultChecker:          ResultChecker,
    workerName:             String
  )(implicit ac: ActorRefFactory): ActorRef
}

object DefaultWorkerFactory extends WorkerFactory {

  override def createWorker(
    queue:                  QueueRef,
    routee:                 ActorRef,
    metricsCollector:       ActorRef,
    circuitBreakerSettings: Option[CircuitBreakerSettings],
    resultChecker:          ResultChecker, workerName: String
  )(implicit ac: ActorRefFactory): ActorRef = {
    ac.actorOf(Worker.default(queue, routee, metricsCollector, circuitBreakerSettings)(resultChecker), workerName)
  }
}

object QueueProcessor {
  private[queue]type WorkerPool = List[WorkerRef] //keep sequence of creation time

  case class ScaleTo(numOfWorkers: Int, reason: Option[String] = None) {
    assert(numOfWorkers >= 0)
  }

  case class RunningStatus(pool: WorkerPool)
  case object ShuttingDown

  case class Shutdown(reportBackTo: Option[ActorRef] = None, timeout: FiniteDuration = 3.minutes)

  private case object ShutdownTimeout
  private case object HealthCheck
  private[queue] case class RouteeRetrieved(routee: ActorRef)
  private[queue] case class RouteeFailed(ex: Throwable)

  def default(
    queue:                  QueueRef,
    backend:                Backend,
    settings:               ProcessingWorkerPoolSettings,
    metricsCollector:       ActorRef,
    circuitBreakerSettings: Option[CircuitBreakerSettings] = None,
    workerFactory:          WorkerFactory                  = DefaultWorkerFactory
  )(resultChecker: ResultChecker): Props =
    Props(new QueueProcessor(
      queue,
      backend,
      settings,
      circuitBreakerSettings,
      metricsCollector,
      workerFactory,
      resultChecker
    )).withDeploy(Deploy.local)

}
