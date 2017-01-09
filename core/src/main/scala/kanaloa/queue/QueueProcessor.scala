package kanaloa.queue

import java.util.concurrent.atomic.AtomicLong

import akka.actor._
import kanaloa.ApiProtocol._
import kanaloa.Sampler.SamplerSettings
import kanaloa.WorkerPoolSampler
import kanaloa.handler.HandlerProvider.{HandlerChange, HandlersAdded}
import kanaloa.handler.{HandlerProvider, Handler}
import kanaloa.metrics.{Reporter, Metric}
import kanaloa.metrics.Metric.PoolSize
import kanaloa.queue.Queue.Retire
import kanaloa.queue.QueueProcessor._
import kanaloa.util.MessageScheduler

import scala.concurrent.duration._

class QueueProcessor[T](
  queue:                QueueRef,
  handlerProvider:      HandlerProvider[T],
  settings:             ProcessingWorkerPoolSettings,
  workerFactory:        WorkerFactory,
  samplerFactory:       WorkerPoolSamplerFactory,
  autothrottlerFactory: Option[AutothrottlerFactory]
) extends Actor with ActorLogging with MessageScheduler {
  val listSelection = new ListSelection
  val healthCheckSchedule = {
    import context.dispatcher
    context.system.scheduler.schedule(settings.healthCheckInterval, settings.healthCheckInterval, self, HealthCheck)
  }

  val metricsCollector: ActorRef = samplerFactory() // todo: life management

  val autoThrottler: Option[ActorRef] = autothrottlerFactory.map(_(self, metricsCollector)) // todo: life management

  var workerCount = 0
  var workerPool: WorkerPool = List[ActorRef]()
  var inflightWorkerCreation = 0
  var inflightWorkerRemoval = 0

  //stop any children which failed.  Let the DeathWatch handle it
  override val supervisorStrategy = SupervisorStrategy.stoppingStrategy

  override def preStart(): Unit = {

    super.preStart()
    metricsCollector ! Metric.PoolSize(0)

    if (!handlerProvider.handlers.isEmpty)
      start()

    handlerProvider.subscribe(self ! _)
    context watch queue
  }

  val starting: Receive = {
    case HandlersAdded(_) ⇒ start()
    case Shutdown(reportTo, timeout) ⇒
      log.info("Commanded to shutdown while starting. Shutting down")
      shutdown(reportTo, timeout)
    case _ ⇒ //ignore everything else when starting
  }

  val receive = starting

  def start(): Unit = {
    (1 to settings.startingPoolSize).foreach(_ ⇒ retrieveHandler())
    context become running
  }

  def currentWorkers = workerPool.size + inflightWorkerCreation - inflightWorkerRemoval

  val running: Receive = {

    case hc: HandlerChange ⇒
      self ! HealthCheck //todo: as a temporary solution mostly for the intermediate implementation to avoid involving involving another design change. This approach would allow the queueProcessor to replenish workers. The real plan, however, is to do one QueueProcessor per handler, which should be implemented before the next release.

    case ScaleTo(newPoolSize, reason) if inflightWorkerCreation == 0 && inflightWorkerRemoval == 0 ⇒
      log.debug(s"Command to scale to $newPoolSize, currently at ${workerPool.size} due to ${reason.getOrElse("no reason given")}")
      val toPoolSize = Math.max(settings.minPoolSize, Math.min(settings.maxPoolSize, newPoolSize))
      val diff = toPoolSize - currentWorkers

      tryCreateWorkersIfNeeded(diff)
      if (diff < 0) {
        inflightWorkerRemoval -= diff
        workerPool.take(-diff).foreach(_ ! Worker.Retire)
      }

    case ScaleTo(_, _) ⇒ //ignore when there is inflight creation or removal going on.

    case hr: HandlerRetrieved[T] ⇒
      createWorker(hr.handler)

    case HandlerProviderFailure(ex) ⇒
      inflightWorkerCreation = Math.max(0, inflightWorkerCreation - 1)
      if (settings.logRouteeRetrievalError)
        log.warning("Failed to retrieve Routee: " + ex.getMessage)

    case Terminated(worker) if workerPool.contains(worker) ⇒
      removeWorker(worker)
      if (workerPool.length < settings.minPoolSize) {
        log.error("Worker death caused worker pool size drop below minimum.")
        if (workerPool.isEmpty && settings.shutdownOnAllWorkerDeath) {
          log.error("Dispatcher is shutdown because there are no workers left.")
          shutdown()
        }
      }

    case HealthCheck ⇒
      metricsCollector ! PoolSize(workerPool.length) //also take the opportunity to report PoolSize, this is needed because statsD metrics report is not reliable
      healthCheck()

    //if the Queue terminated, time to shut stuff down.
    case Terminated(`queue`) ⇒
      log.debug(s"Queue ${queue.path} is terminated")
      healthCheckSchedule.cancel()
      if (workerPool.isEmpty) {
        context stop self
      } else {
        workerPool.foreach(_ ! Worker.Retire)
        delayedMsg(settings.defaultShutdownTimeout, ShutdownTimeout)
        context become shuttingDown(None)
      }

    case qs: QueryStatus ⇒ qs reply RunningStatus(workerPool)

    //queue processor initiates shutdown of everyone.
    case Shutdown(reportTo, timeout) ⇒
      log.info("Commanded to shutdown. Shutting down")
      shutdown(reportTo, timeout)
  }

  def shutdown(reportTo: Option[ActorRef] = None, timeout: FiniteDuration = settings.defaultShutdownTimeout): Unit = {
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
    inflightWorkerRemoval = Math.max(0, inflightWorkerRemoval - 1)
    context.unwatch(worker)
    workerPool = workerPool.filter(_ != worker)
    metricsCollector ! Metric.PoolSize(workerPool.length)
  }

  private def retrieveHandler(): Unit = {
    val handlers = handlerProvider.handlers
    listSelection.select(handlers) foreach { handler ⇒
      self ! HandlerRetrieved(handler)
      inflightWorkerCreation += 1
    }

  }

  private def healthCheck(): Unit = {
    if (tryCreateWorkersIfNeeded(settings.minPoolSize - currentWorkers))
      log.debug("Number of workers in pool is below minimum. Trying to replenish.")
  }
  /**
   *
   * @param numberOfWorkersToCreate
   * @return true if workers are scheduled to be created
   */
  private def tryCreateWorkersIfNeeded(numberOfWorkersToCreate: Int): Boolean = {
    val workerNeeded = numberOfWorkersToCreate > 0
    if (workerNeeded)
      (1 to numberOfWorkersToCreate).foreach(_ ⇒ retrieveHandler())
    workerNeeded
  }

  private def createWorker(handler: Handler[T]): Unit = {
    val workerName = s"worker-$workerCount"
    workerCount += 1
    val worker = workerFactory(queue, handler, metricsCollector, workerName)
    context watch worker

    workerPool = workerPool :+ worker
    metricsCollector ! Metric.PoolSize(workerPool.length)
    inflightWorkerCreation = Math.max(0, inflightWorkerCreation - 1)
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

object QueueProcessor {
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
    def apply(circuitBreakerSettings: Option[CircuitBreakerSettings]): WorkerFactory = new WorkerFactory {
      def apply[T](q: QueueRef, h: Handler[T], mc: QueueRef, name: String)(implicit ac: ActorRefFactory): QueueRef = {
        ac.actorOf(Worker.default(q, h, mc, circuitBreakerSettings), name)
      }
    }
  }

  private[kanaloa] trait WorkerPoolSamplerFactory {
    def apply()(implicit ac: ActorRefFactory): ActorRef
  }

  private[kanaloa] object WorkerPoolSamplerFactory {
    def apply(queueSampler: ActorRef, settings: SamplerSettings, reporter: Option[Reporter]): WorkerPoolSamplerFactory = new WorkerPoolSamplerFactory {
      def apply()(implicit ac: ActorRefFactory) =
        ac.actorOf(WorkerPoolSampler.props(reporter, queueSampler, settings))
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

  case class RunningStatus(pool: WorkerPool)
  case object ShuttingDown

  case class Shutdown(reportBackTo: Option[ActorRef] = None, timeout: FiniteDuration = 3.minutes)

  private case object ShutdownTimeout
  private case object HealthCheck
  private[queue] case class HandlerRetrieved[T](handler: Handler[T])
  private[queue] case class HandlerProviderFailure(ex: Throwable)

  def default[T](
    queue:                QueueRef,
    handlerProvider:      HandlerProvider[T],
    settings:             ProcessingWorkerPoolSettings,
    workerFactory:        WorkerFactory,
    samplerFactory:       WorkerPoolSamplerFactory,
    autothrottlerFactory: Option[AutothrottlerFactory]

  ): Props = {
    Props(new QueueProcessor(
      queue,
      handlerProvider,
      settings,
      workerFactory,
      samplerFactory,
      autothrottlerFactory
    )).withDeploy(Deploy.local)
  }

}
