package kanaloa.queue

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import akka.actor._
import kanaloa.ApiProtocol._
import kanaloa.handler.Handler
import kanaloa.metrics.{Metric, Reporter}
import kanaloa.queue.Sampler.SamplerSettings
import kanaloa.queue.Worker.{CancelDelay, DelayBeforeNextWork}
import kanaloa.queue.WorkerPoolManager._
import kanaloa.util.AnyEq._
import kanaloa.util.{MessageScheduler, Naming}

import scala.concurrent.duration._

class WorkerPoolManager[T](
  queue:                 QueueRef,
  handler:               Handler[T],
  settings:              WorkerPoolSettings,
  workerFactory:         WorkerFactory,
  samplerFactory:        WorkerPoolSamplerFactory,
  autothrottlerFactory:  Option[AutothrottlerFactory],
  circuitBreakerFactory: Option[CircuitBreakerFactory],
  baseReporter:          Option[Reporter]
) extends Actor with ActorLogging with MessageScheduler {

  val reporter: Option[Reporter] = {
    val handlerPrefix = Naming.sanitizeActorName(handler.name)
    baseReporter.map(_.withNewPrefix(_ + "." + handlerPrefix))
  }

  val circuitBreaker = circuitBreakerFactory.map(_(self, reporter))

  val listSelection = new ListSelection

  val metricsCollector: ActorRef = samplerFactory(reporter, circuitBreaker)

  val autoThrottler: Option[ActorRef] = autothrottlerFactory.map(_(self, metricsCollector))
  var workerCount = 0

  var workerPool: WorkerPool = Nil

  var inflightWorkerRemoval = 0

  def reportPoolSize() =
    metricsCollector ! Metric.PoolSize(workerPool.length)

  override def preStart(): Unit = {
    super.preStart()
    reportPoolSize()
    tryCreateWorkersIfNeeded(settings.startingPoolSize)
  }

  def currentWorkers = workerPool.size - inflightWorkerRemoval

  val receive: Receive = {
    case kanaloa.handler.Terminate ⇒
      log.warning("Handler requested to terminate worker pool")
      shutdown()

    case kanaloa.handler.Hold(duration) ⇒
      workerPool.foreach(_ ! DelayBeforeNextWork(duration))

    case UnHold ⇒
      workerPool.foreach(_ ! CancelDelay)

    case ScaleTo(newPoolSize, reason) if inflightWorkerRemoval === 0 ⇒
      log.debug(s"Command to scale to $newPoolSize, currently at ${workerPool.size} due to ${reason.getOrElse("no reason given")}")
      val toPoolSize = Math.max(settings.minPoolSize, Math.min(settings.maxPoolSize, newPoolSize))
      val diff = toPoolSize - currentWorkers

      tryCreateWorkersIfNeeded(diff)
      if (diff < 0) {
        inflightWorkerRemoval -= diff
        workerPool.take(-diff).foreach(_ ! Worker.Retire)
      }

    case ScaleTo(_, _) ⇒ //ignore when there is inflight removal going on.

    case Terminated(worker) if workerPool.contains(worker) ⇒
      removeWorker(worker)
      if (workerPool.length < settings.minPoolSize) {
        log.error("Worker death caused worker pool size drop below minimum.")
        delayedMsg(settings.replenishSpeed, HealthCheck) //delayed so that it won't run into a uncontrolled create-die-create cycle.
      }

    case HealthCheck ⇒
      reportPoolSize() //also take the opportunity to report PoolSize, this is needed because statsD metrics report is not reliable
      if (!tryCreateWorkersIfNeeded(settings.minPoolSize - currentWorkers).isEmpty)
        log.debug("Number of workers in pool is below minimum. Replenished.")

    case qs: QueryStatus ⇒ qs reply RunningStatus(workerPool)

    //worker pool initiates shutdown of everyone.
    case Shutdown(reportTo, timeout) ⇒
      log.info("Commanded to shutdown. Shutting down")
      shutdown(reportTo, timeout)
  }

  def shutdown(reportTo: Option[ActorRef] = None, timeout: FiniteDuration = settings.defaultShutdownTimeout): Unit = {
    if (workerPool.isEmpty) {
      reportTo.foreach(_ ! ShutdownSuccessfully)
      context stop self
    } else {
      workerPool.foreach(_ ! Worker.Retire)
      delayedMsg(timeout, ShutdownTimeout)
      context become shuttingDown(reportTo)
    }
  }

  def shuttingDown(reportTo: Option[ActorRef]): Receive = {
    def tryFinish(): Unit = {
      if (workerPool.isEmpty) {
        log.info(s"All Workers have terminated, WorkerPoolManager is shutting down")
        reportTo.foreach(_ ! ShutdownSuccessfully)
        context stop self
      }
    }
    {
      case Terminated(worker) if workerPool.contains(worker) ⇒
        removeWorker(worker)
        tryFinish()

      case qs: QueryStatus ⇒ qs reply ShuttingDown

      case ShutdownTimeout ⇒
        log.warning("Shutdown timed out, forcefully shutting down")
        reportTo.foreach(_ ! ShutdownForcefully)
        context stop self

      case m ⇒ log.info("Unhandled message received and ignored during shutdown: " + m)

    }: Receive
  }

  private def removeWorker(worker: ActorRef): Unit = {
    inflightWorkerRemoval = Math.max(0, inflightWorkerRemoval - 1)
    context.unwatch(worker)
    workerPool = workerPool.filter(_ != worker)
    reportPoolSize()
  }

  /**
   *
   * @param numberOfWorkersToCreate
   * @return workers created
   */
  private def tryCreateWorkersIfNeeded(numberOfWorkersToCreate: Int): List[ActorRef] = {
    val workerNeeded = numberOfWorkersToCreate > 0
    if (workerNeeded)
      (1 to numberOfWorkersToCreate).toList.map(_ ⇒ createWorker())
    else
      Nil
  }

  private def createWorker(): ActorRef = {
    val workerName = s"worker-$workerCount"
    workerCount += 1
    val worker = workerFactory(queue, handler, metricsCollector, workerName)
    context watch worker

    workerPool = workerPool :+ worker
    reportPoolSize()
    worker
  }
}

private[queue] class ListSelection {
  val next = new AtomicLong
  def select[T](list: Seq[T]): Option[T] = {
    if (list.nonEmpty) {
      val size = list.size
      val index = (next.getAndIncrement % size).asInstanceOf[Int]
      Some(list(if (index < 0) size + index - 1 else index))
    } else None
  }
}

object WorkerPoolManager {
  private[queue]type WorkerPool = List[WorkerRef] //keep sequence of creation time

  private[kanaloa] trait WorkerFactory {
    def apply[T](
      queueRef:         ActorRef,
      handler:          Handler[T],
      metricsCollector: ActorRef,
      workerName:       String
    )(implicit ac: ActorRefFactory): ActorRef
  }

  private[kanaloa] object WorkerFactory {
    val default: WorkerFactory = new WorkerFactory {
      def apply[T](q: QueueRef, h: Handler[T], mc: QueueRef, name: String)(implicit ac: ActorRefFactory): QueueRef = {
        ac.actorOf(Worker.default(q, h, mc), name)
      }
    }
  }

  private[kanaloa] trait CircuitBreakerFactory {
    def apply(
      workerPoolManagerRef: WorkerPoolManagerRef,
      reporter:             Option[Reporter]
    )(implicit arf: ActorRefFactory): ActorRef
  }

  private[kanaloa] object CircuitBreakerFactory {
    def apply(settings: CircuitBreakerSettings): CircuitBreakerFactory = new CircuitBreakerFactory {
      def apply(
        workerPoolManagerRef: WorkerPoolManagerRef,
        reporter:             Option[Reporter]
      )(implicit arf: ActorRefFactory): ActorRef =
        arf.actorOf(CircuitBreaker.props(workerPoolManagerRef, settings, reporter), "circuit-breaker")
    }
  }

  private[kanaloa] trait WorkerPoolSamplerFactory {
    def apply(reporter: Option[Reporter], metricsForwardTo: Option[ActorRef])(implicit ac: ActorRefFactory): ActorRef
  }

  private[kanaloa] object WorkerPoolSamplerFactory {
    val index = new AtomicInteger(0)
    def apply(
      queueSampler: ActorRef,
      settings:     SamplerSettings
    ): WorkerPoolSamplerFactory = new WorkerPoolSamplerFactory {
      def apply(reporter: Option[Reporter], metricsForwardTo: Option[ActorRef])(implicit arf: ActorRefFactory) = {
        arf.actorOf(WorkerPoolSampler.props(reporter, queueSampler, settings, metricsForwardTo), "sampler" + index.incrementAndGet())
      }
    }
  }

  private[kanaloa] trait AutothrottlerFactory {
    def apply(workerPoolManager: ActorRef, workerPoolSampler: ActorRef)(implicit ac: ActorRefFactory): ActorRef
  }

  private[kanaloa] object AutothrottlerFactory {
    def apply(settings: AutothrottleSettings): AutothrottlerFactory = new AutothrottlerFactory {
      def apply(workerPoolManager: ActorRef, workerPoolSampler: ActorRef)(implicit ac: ActorRefFactory): ActorRef =
        ac.actorOf(Autothrottler.default(workerPoolManager, settings, workerPoolSampler), "autothrottler")
    }
  }

  case class ScaleTo(numOfWorkers: Int, reason: Option[String] = None) {
    assert(numOfWorkers >= 0)
  }

  case object UnHold

  case class RunningStatus(pool: WorkerPool)
  case object ShuttingDown

  case class Shutdown(reportBackTo: Option[ActorRef] = None, timeout: FiniteDuration = 3.minutes)

  private case object ShutdownTimeout
  private case object HealthCheck

  def default[T](
    queue:                 QueueRef,
    handler:               Handler[T],
    settings:              WorkerPoolSettings,
    workerFactory:         WorkerFactory,
    samplerFactory:        WorkerPoolSamplerFactory,
    autothrottlerFactory:  Option[AutothrottlerFactory],
    circuitBreakerFactory: Option[CircuitBreakerFactory],
    baseReporter:          Option[Reporter]
  ): Props = {
    Props(new WorkerPoolManager(
      queue,
      handler,
      settings,
      workerFactory,
      samplerFactory,
      autothrottlerFactory,
      circuitBreakerFactory,
      baseReporter
    )).withDeploy(Deploy.local)
  }

}
