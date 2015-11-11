package kanaloa.reactive.dispatcher.queue

import java.time.{ LocalDateTime, ZoneOffset }

import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import kanaloa.reactive.dispatcher.ApiProtocol._
import kanaloa.reactive.dispatcher.metrics.{ Metric, MetricsCollector, NoOpMetricsCollector }
import kanaloa.reactive.dispatcher.queue.Queue.Retire
import kanaloa.reactive.dispatcher.queue.QueueProcessor._
import kanaloa.reactive.dispatcher.queue.Worker.Hold
import kanaloa.reactive.dispatcher.{ Backend, ResultChecker }
import kanaloa.util.FiniteCollection._
import kanaloa.util.MessageScheduler

import scala.concurrent.duration._

trait QueueProcessor extends Actor with ActorLogging with MessageScheduler {
  import QueueProcessor.WorkerPool
  val queue: QueueRef
  def backend: Backend
  def settings: ProcessingWorkerPoolSettings
  def resultChecker: ResultChecker
  val metricsCollector: MetricsCollector
  type ResultHistory = Vector[Boolean]
  val resultHistoryLength: Int
  protected def onWorkError(resultHistory: ResultHistory, pool: WorkerPool)

  metricsCollector.send(Metric.PoolSize(settings.startingPoolSize))

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minute) {
      case _: Exception ⇒ Restart
    }

  protected def workerProp(queueRef: QueueRef): Props = Worker.default(queue, backend)(resultChecker)

  def receive: Receive = {
    val workers = (1 to settings.startingPoolSize).map(createWorker).toSet
    settings.maxProcessingTime.foreach(delayedMsg(_, QueueMaxProcessTimeReached(queue)))
    context watch queue

    monitoring()(workers)
  }

  def monitoring(resultHistory: ResultHistory = Vector.empty)(pool: WorkerPool): Receive = {
    def workError(): Unit = {
      val newHistory = resultHistory.enqueueFinite(false, resultHistoryLength)
      context become monitoring(newHistory)(pool)
      onWorkError(newHistory, pool)
    }

    {
      case ScaleTo(newPoolSize, reason) ⇒
        log.debug(s"Command to scale to $newPoolSize, currently at ${pool.size} due to ${reason.getOrElse("no reason given")}")
        metricsCollector.send(Metric.PoolSize(newPoolSize))
        val toPoolSize = Math.max(settings.minPoolSize, Math.min(settings.maxPoolSize, newPoolSize))
        val diff = toPoolSize - pool.size
        if (diff > 0)
          context become monitoring(resultHistory)(pool ++ (1 to diff).map(createWorker))
        else if (diff < 0)
          pool.take(-diff).foreach(_ ! Worker.Retire)

      case WorkCompleted(worker, duration) ⇒
        context become monitoring(resultHistory.enqueueFinite(true, resultHistoryLength))(pool)
        metricsCollector.send(Metric.ProcessTime(duration))
        metricsCollector.send(Metric.WorkCompleted)

      case WorkFailed(_) ⇒
        workError()
        metricsCollector.send(Metric.WorkFailed)

      case WorkTimedOut(_) ⇒
        workError()
        metricsCollector.send(Metric.WorkTimedOut)

      case Terminated(worker) if pool.contains(worker) ⇒
        removeWorker(pool, worker, monitoring(resultHistory), "Worker removed")

      case Terminated(`queue`) ⇒
        log.debug(s"Queue ${queue.path} is terminated")
        self ! Shutdown(retireQueue = false)

      case QueueMaxProcessTimeReached(queue) ⇒
        log.warning(s"Queue ${queue.path} is still processing after max process time. Shutting Down")
        self ! Shutdown(retireQueue = true)

      case qs: QueryStatus ⇒ qs reply RunningStatus(pool)

      case Shutdown(reportTo, timeout, retireQueue) ⇒
        log.info("Commanded to shutdown. Shutting down")
        if (retireQueue)
          queue ! Retire(timeout)
        else //retire from the workers' side
          pool.foreach(_ ! Worker.Retire)

        delayedMsg(timeout, ShutdownTimeout)
        context become shuttingDown(pool, reportTo)
    }: Receive
  }

  def shuttingDown(pool: WorkerPool, reportTo: Option[ActorRef]): Receive = {

    case Terminated(worker) if pool.contains(worker) ⇒
      removeWorker(pool, worker, shuttingDown(_, reportTo), "successfully after command", reportTo)

    case Terminated(_)   ⇒ //ignore other termination

    case qs: QueryStatus ⇒ qs reply ShuttingDown

    case ShutdownTimeout ⇒
      log.warning("Shutdown timed out, forcefully shutting down")
      pool.foreach(_ ! PoisonPill)
      context stop self

    case _ ⇒ //Ignore
  }

  private def createWorker(index: Int): WorkerRef = {
    val timestamp = LocalDateTime.now.toInstant(ZoneOffset.UTC).toEpochMilli
    val worker = context.actorOf(
      workerProp(queue),
      s"worker-${queue.path.name}-$index-${timestamp}"
    )
    context watch worker
    worker
  }

  private def removeWorker(
    pool:              WorkerPool,
    worker:            WorkerRef,
    nextContext:       WorkerPool ⇒ Receive,
    finishWithMessage: String               = "",
    reportToOnFinish:  Option[ActorRef]     = None
  ): Unit = {
    context unwatch worker
    val newPool = pool - worker
    if (!finishIfPoolIsEmpty(newPool, finishWithMessage, reportToOnFinish))
      context become nextContext(newPool)

  }

  private def finishIfPoolIsEmpty(
    pool:        WorkerPool,
    withMessage: String,
    reportTo:    Option[ActorRef] = None
  ): Boolean = {
    val finishes = pool.isEmpty
    if (finishes) {
      log.info(s"Queue Processor is shutdown $withMessage")
      reportTo.foreach(_ ! ShutdownSuccessfully)
      context stop self
    }
    finishes
  }

}

/**
 * The default queue processor uses the same [[ ResultChecker ]] for all queues
 * @param resultChecker
 */
case class DefaultQueueProcessor(
  queue:            QueueRef,
  backend:          Backend,
  settings:         ProcessingWorkerPoolSettings,
  metricsCollector: MetricsCollector             = NoOpMetricsCollector,
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
  metricsCollector:       MetricsCollector             = NoOpMetricsCollector,
  resultChecker:          ResultChecker
) extends QueueProcessor {

  override val resultHistoryLength: Int = circuitBreakerSettings.historyLength

  override protected def onWorkError(resultHistory: ResultHistory, pool: WorkerPool): Unit = {
    if ((resultHistory.count(r ⇒ !r).toDouble / resultHistoryLength) >= circuitBreakerSettings.errorRateThreshold) {
      metricsCollector.send(Metric.CircuitBreakerOpened)
      pool.foreach(_ ! Hold(circuitBreakerSettings.closeDuration))
    }
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
  case class Shutdown(reportBackTo: Option[ActorRef] = None, timeout: FiniteDuration = 3.minutes, retireQueue: Boolean = true)
  private case object ShutdownTimeout

  def default(
    queue:            QueueRef,
    backend:          Backend,
    settings:         ProcessingWorkerPoolSettings,
    metricsCollector: MetricsCollector             = NoOpMetricsCollector
  )(resultChecker: ResultChecker): Props =
    Props(new DefaultQueueProcessor(
      queue,
      backend,
      settings,
      metricsCollector,
      resultChecker
    ))

  def withCircuitBreaker(
    queue:                  QueueRef,
    backend:                Backend,
    settings:               ProcessingWorkerPoolSettings,
    circuitBreakerSettings: CircuitBreakerSettings,
    metricsCollector:       MetricsCollector             = NoOpMetricsCollector
  )(resultChecker: ResultChecker): Props =
    Props(new QueueProcessorWithCircuitBreaker(
      queue,
      backend,
      settings,
      circuitBreakerSettings,
      metricsCollector,
      resultChecker
    ))
}

