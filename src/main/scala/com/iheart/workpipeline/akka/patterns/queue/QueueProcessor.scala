package com.iheart.workpipeline.akka.patterns.queue

import java.time.{ZoneOffset, LocalDateTime}

import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import com.iheart.workpipeline.metrics.{Metric, MetricsCollector, NoOpMetricsCollector}
import com.iheart.workpipeline.akka.helpers.MessageScheduler
import com.iheart.workpipeline.akka.patterns.CommonProtocol.{ShutdownSuccessfully, QueryStatus}
import QueueProcessor._
import Queue.{Retire}
import scala.concurrent.duration._

trait QueueProcessor extends Actor with ActorLogging with MessageScheduler {
  import QueueProcessor.WorkerPool
  val queue: QueueRef
  def delegateeProps: Props
  def settings: ProcessingWorkerPoolSettings
  val metricsCollector: MetricsCollector

  metricsCollector.send(Metric.PoolSize(settings.startingPoolSize))

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minute) {
      case _: Exception => Restart
    }

  def workerProp(queueRef: QueueRef, delegateeProps: Props): Props

  def receive: Receive = {
    val workers = (1 to settings.startingPoolSize).map(createWorker).toSet
    settings.maxProcessingTime.foreach(delayedMsg(_, QueueMaxProcessTimeReached(queue)))
    context watch queue

    monitoring(workers)
  }


  def monitoring(pool: WorkerPool): Receive = {

    case ScaleTo(newPoolSize, reason) =>
      log.info(s"Command to scale to $newPoolSize, currently at ${pool.size} due to ${reason.getOrElse("no reason given")}")
      metricsCollector.send(Metric.PoolSize(newPoolSize))

      val diff = newPoolSize - pool.size
      if (diff > 0)
        context become monitoring(pool ++ (1 to diff).map(createWorker))
      else if (diff < 0 && newPoolSize >= settings.minPoolSize)
        pool.take(-diff).foreach(_ ! Worker.Retire)

    case MissionAccomplished(worker) =>
      removeWorker(pool, worker, monitoring, "successfully after all work is done")

    case Terminated(worker) if pool.contains(worker) =>
      removeWorker(pool, worker, monitoring, "unexpected termination when all workers retired")

    case Terminated(`queue`) =>
      log.info(s"Queue ${queue.path} is terminated")
      self ! Shutdown(retireQueue = false)

    case QueueMaxProcessTimeReached(queue) =>
      log.warning(s"Queue ${queue.path} is still processing after max process time. Shutting Down")
      self ! Shutdown(retireQueue = true)

    case qs: QueryStatus => qs reply RunningStatus(pool)

    case Shutdown(reportTo, timeout, retireQueue) =>
      log.info("Commanded to shutdown. Shutting down")
      if(retireQueue)
        queue ! Retire(timeout)
      else //retire from the workers' side
        pool.foreach(_ ! Worker.Retire)

      delayedMsg(timeout, ShutdownTimeout)
      context become shuttingDown(pool, reportTo)

  }

  def shuttingDown(pool: WorkerPool, reportTo: Option[ActorRef]): Receive = {
    case MissionAccomplished(worker) =>
      removeWorker(pool, worker, shuttingDown(_, reportTo), "successfully after command", reportTo)

    case Terminated(worker) if pool.contains(worker) =>
      removeWorker(pool, worker, shuttingDown(_, reportTo), "successfully after command", reportTo)


    case Terminated(_) => //ignore other termination

    case qs: QueryStatus => qs reply ShuttingDown

    case ShutdownTimeout =>
      log.error("Shutdown timed out, forcefully shutting down")
      pool.foreach(_ ! PoisonPill)
      context stop self

    case _ => sender ! ShuttingDown
  }

  private def createWorker(index: Int): WorkerRef = {
    val timestamp = LocalDateTime.now.toInstant(ZoneOffset.UTC).toEpochMilli
    val worker = context.actorOf(workerProp(queue, delegateeProps),
                                 s"worker-${queue.path.name}-$index-${timestamp}")
    context watch worker
    worker
  }

  private def removeWorker(pool: WorkerPool,
                           worker: WorkerRef,
                           nextContext: WorkerPool => Receive,
                           finishWithMessage: String = "",
                           reportToOnFinish: Option[ActorRef] = None): Unit = {
    context unwatch worker
    val newPool = pool - worker
    if(!finishIfPoolIsEmpty(newPool,finishWithMessage, reportToOnFinish))
      context become nextContext(newPool)

  }

  private def finishIfPoolIsEmpty(pool: WorkerPool,
                                  withMessage: String,
                                  reportTo: Option[ActorRef] = None): Boolean = {
    val finishes = pool.isEmpty
    if(finishes) {
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
case class DefaultQueueProcessor(queue: QueueRef,
                                 delegateeProps: Props,
                                 settings: ProcessingWorkerPoolSettings,
                                 metricsCollector: MetricsCollector = NoOpMetricsCollector,
                                 resultChecker: ResultChecker) extends QueueProcessor {
  def workerProp(queue: QueueRef, delegateeProps: Props): Props =
    Worker.default(queue, delegateeProps)(resultChecker)
}

case class QueueProcessorWithCircuitBreaker(queue: QueueRef,
                                            delegateeProps: Props,
                                            settings: ProcessingWorkerPoolSettings,
                                            circuitBreakerSettings: CircuitBreakerSettings,
                                            metricsCollector: MetricsCollector = NoOpMetricsCollector,
                                            resultChecker: ResultChecker) extends QueueProcessor {
  def workerProp(queue: QueueRef, delegateeProps: Props): Props =
    Worker.withCircuitBreaker(queue, delegateeProps, circuitBreakerSettings)(resultChecker)

}


object QueueProcessor {
  private[queue] type WorkerPool = Set[WorkerRef]


  case class ScaleTo(numOfWorkers: Int, reason: Option[String] = None) {
    assert(numOfWorkers > 0)
  }


  case class MissionAccomplished(worker: WorkerRef)
  case class QueueMaxProcessTimeReached(queue: QueueRef)
  case class RunningStatus(pool: WorkerPool)
  case object ShuttingDown
  case class Shutdown(reportBackTo: Option[ActorRef] = None, timeout: FiniteDuration = 3.minutes, retireQueue: Boolean = true)
  private case object ShutdownTimeout

  def default(queue: QueueRef,
              delegateeProps: Props,
              settings: ProcessingWorkerPoolSettings,
              metricsCollector: MetricsCollector = NoOpMetricsCollector)(resultChecker: ResultChecker): Props =
    Props(new DefaultQueueProcessor(queue,
                                    delegateeProps,
                                    settings,
                                    metricsCollector,
                                    resultChecker))

  def withCircuitBreaker(queue: QueueRef,
                         delegateeProps: Props,
                         settings: ProcessingWorkerPoolSettings,
                         circuitBreakerSettings: CircuitBreakerSettings,
                         metricsCollector: MetricsCollector = NoOpMetricsCollector)(resultChecker: ResultChecker): Props =
    Props(new QueueProcessorWithCircuitBreaker(queue,
                                               delegateeProps,
                                               settings,
                                               circuitBreakerSettings,
                                               metricsCollector,
                                               resultChecker))
}

