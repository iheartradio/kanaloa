package kanaloa

import akka.actor.SupervisorStrategy.Escalate
import akka.actor._
import com.typesafe.config.{Config, ConfigFactory}
import kanaloa.ApiProtocol._
import kanaloa.Dispatcher.{ShutdownTimedOut, UnSubscribePerformanceMetrics, SubscribePerformanceMetrics, Settings}
import kanaloa.PushingDispatcher.GracePeriodDone
import kanaloa.queue._
import Regulator.DroppingRate
import kanaloa.handler.HandlerProvider.{HandlersRemoved, HandlersAdded}
import kanaloa.handler.{Handler, HandlerProviderAdaptor, HandlerProvider}
import kanaloa.metrics.{StatsDClient, Metric, Reporter}
import kanaloa.queue.Queue.{Retire, Enqueue, EnqueueRejected}
import kanaloa.queue.WorkerPoolManager.{CircuitBreakerFactory, WorkerPoolSamplerFactory, WorkerFactory, AutothrottlerFactory}
import kanaloa.queue._
import kanaloa.util.MessageScheduler
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.readers.namemappers.implicits.hyphenCase
import net.ceedubs.ficus.readers.ValueReader
import kanaloa.util.AnyEq._
import scala.concurrent.duration._
import scala.reflect._
import scala.util.Random

trait Dispatcher[T] extends Actor with ActorLogging with MessageScheduler {
  def name: String
  def settings: Dispatcher.Settings
  def handlerProvider: HandlerProvider[T]
  def reporter: Option[Reporter]

  type HandlerName = String
  type WorkerPoolRef = ActorRef

  protected lazy val queueSampler = context.actorOf(QueueSampler.props(reporter, settings.samplerSettings), "queueSampler")

  protected def queueProps: Props

  protected lazy val queue = context.actorOf(queueProps, "queue")

  context watch queue
  override val supervisorStrategy =
    OneForOneStrategy() {
      case e ⇒
        log.error(s"Dispatcher $name one of the children actor died due to $e")
        Escalate
    }

  var workerPoolIndex: Int = 0

  var workerPools: List[(HandlerName, WorkerPoolRef)] = handlerProvider.handlers.map(createWorkerPool)

  var inGracePeriod = workerPools.isEmpty

  if (inGracePeriod) //during initial grace period request (will be placed in queue) won't get rejected even when the backend is not online yet.
    delayedMsg(settings.initialGracePeriod, GracePeriodDone)

  handlerProvider.subscribe(self ! _)

  private def createWorkerPool(handler: Handler[T]): (HandlerName, WorkerPoolRef) = {
    val props = WorkerPoolManager.default(
      queue,
      handler,
      settings.workerPool,
      WorkerFactory.default,
      WorkerPoolSamplerFactory(queueSampler, settings.samplerSettings),
      settings.autothrottle.map(AutothrottlerFactory.apply),
      settings.circuitBreaker.map(CircuitBreakerFactory.apply),
      reporter
    )

    val workerPool = context.actorOf(props, s"worker-pool-manager-$workerPoolIndex")
    context watch workerPool
    workerPoolIndex += 1
    handler.name → workerPool
  }

  def shutdown(reportTo: Option[ActorRef], timeout: FiniteDuration): Unit = {
    if (workerPools.isEmpty) {
      reportTo.foreach(_ ! ShutdownSuccessfully)
      context stop self
    } else {
      context become shuttingDown(reportTo, timeout)
      import context.dispatcher
      context.system.scheduler.scheduleOnce(timeout, self, ShutdownTimedOut)
      workerPools.foreach(_._2 ! WorkerPoolManager.Shutdown(Some(self), timeout)) //todo: do not timeout workerpool shutdown
    }
  }

  def receive: Receive = ({
    case ShutdownGracefully(reportBack, timeout) ⇒
      queue ! Retire
      shutdown(reportBack, timeout)
    case GracePeriodDone ⇒ inGracePeriod = false
    case Terminated(`queue`) ⇒ //todo: this is only expected in pulling dispatcher. so should make it more safe in pushing( like restart queue)
      shutdown(None, settings.workerPool.defaultShutdownTimeout)

    case SubscribePerformanceMetrics(actor)   ⇒ queueSampler ! Sampler.Subscribe(actor)
    case UnSubscribePerformanceMetrics(actor) ⇒ queueSampler ! Sampler.Unsubscribe(actor)

    case HandlersAdded(handlers: List[Handler[T]]) ⇒
      val (dups, rest) = handlers.partition(h ⇒ workerPools.exists(_._1 === h.name))
      workerPools = workerPools ++ rest.map(createWorkerPool)
      inGracePeriod = false //out of grace period as soon as we see the first handler
      if (!dups.isEmpty)
        log.error(s"New Handler received but their names are duplicates ${dups.map(_.name).mkString(",")}")
      log.info(s"New handlers: ${handlers.map(_.name).mkString(", ")} added.")

    case HandlersRemoved(handlers: List[Handler[T]]) ⇒
      val removedNames = handlers.map(_.name)
      workerPools.collect {
        case (handlerName, workerPool) if removedNames.contains(handlerName) ⇒
          workerPool ! WorkerPoolManager.Shutdown() //todo: make the shutdown timeout configuratble
      }

  }: Receive) orElse monitorWorkerPools(None, false) orElse extraReceive

  def monitorWorkerPools(reportTo: Option[ActorRef], shuttingDown: Boolean): Receive = {
    case Terminated(t) if workerPools.exists(_._2 === t) ⇒
      workerPools = workerPools.filterNot(_._2 === t)
      if (workerPools.isEmpty) {
        if (shuttingDown) {
          context stop self
          reportTo.foreach(_ ! ShutdownSuccessfully)
          log.info(s"Dispatcher $name is shutdown gracefully")
        } else
          log.info(s"All handlers for Dispatcher $name stopped.")
      }

  }

  def shuttingDown(reportTo: Option[ActorRef], timeout: FiniteDuration): Receive = monitorWorkerPools(reportTo, true) orElse {
    case ShutdownTimedOut ⇒
      reportTo.foreach(_ ! ShutdownForcefully)
      log.error(s"Dispatcher $name is shutdown forcefully after timeout of $timeout")
      context stop self
  }

  def extraReceive: Receive = PartialFunction.empty

}

object Dispatcher {

  private[kanaloa] case class SubscribePerformanceMetrics(actor: ActorRef)
  private[kanaloa] case class UnSubscribePerformanceMetrics(actor: ActorRef)
  private case object ShutdownTimedOut

  case class Settings(
    workTimeout:               FiniteDuration                 = 1.minute,
    workRetry:                 Int                            = 0,
    updateInterval:            FiniteDuration                 = 1.second,
    lengthOfDisplayForMessage: Int                            = 200,
    initialGracePeriod:        FiniteDuration                 = 1.second,
    workerPool:                WorkerPoolSettings,
    regulator:                 Option[Regulator.Settings],
    circuitBreaker:            Option[CircuitBreakerSettings],
    autothrottle:              Option[AutothrottleSettings]
  ) {
    val samplerSettings = Sampler.SamplerSettings(updateInterval)
    lazy val workSettings = WorkSettings(workRetry, workTimeout, lengthOfDisplayForMessage)
  }

  //todo: move this out of Dispatcher object
  private[kanaloa] def kanaloaConfig(rootConfig: Config = ConfigFactory.empty) = {
    val referenceConfig = ConfigFactory.defaultReference(getClass.getClassLoader).getConfig("kanaloa")

    rootConfig.as[Option[Config]]("kanaloa")
      .getOrElse(ConfigFactory.empty())
      .withFallback(referenceConfig)
  }

  def defaultDispatcherConfig(
    config:            Config         = kanaloaConfig(),
    defaultConfigName: Option[String] = None
  ): Config = {
    val rootDefaultCfg = config.as[Config]("default-dispatcher")
    defaultConfigName.fold(rootDefaultCfg)(config.as[Config](_).withFallback(rootDefaultCfg))
  }

  def defaultDispatcherSettings(config: Config = kanaloaConfig()): Dispatcher.Settings =
    toDispatcherSettings(defaultDispatcherConfig(config))

  private def toDispatcherSettings(config: Config): Dispatcher.Settings = {
    val settings = config.atPath("root").as[Dispatcher.Settings]("root")
    settings.copy(
      regulator = readComponent[Regulator.Settings]("back-pressure", config),
      circuitBreaker = readComponent[CircuitBreakerSettings]("circuit-breaker", config),
      autothrottle = readComponent[AutothrottleSettings]("autothrottle", config)
    )
  }

  private def readComponent[SettingT: ValueReader](name: String, config: Config): Option[SettingT] =
    for {
      componentCfg ← config.as[Option[Config]](name)
      enabled ← componentCfg.as[Option[Boolean]]("enabled")
      settings ← componentCfg.atPath("root").as[Option[SettingT]]("root") if enabled
    } yield settings

  def readConfig(
    dispatcherName:    String,
    rootConfig:        Config,
    statsDClient:      Option[StatsDClient],
    defaultConfigName: Option[String]       = None
  ): (Settings, Option[Reporter]) = {
    val cfg = kanaloaConfig(rootConfig)
    val dispatcherCfg = cfg.as[Option[Config]]("dispatchers." + dispatcherName).getOrElse(ConfigFactory.empty).withFallback(defaultDispatcherConfig(cfg, defaultConfigName))
    val settings = toDispatcherSettings(dispatcherCfg)

    (settings, Reporter.fromConfig(dispatcherName: String, dispatcherCfg, statsDClient))
  }

}

case class PushingDispatcher[T: ClassTag](
  name:            String,
  settings:        Settings,
  handlerProvider: HandlerProvider[T],
  reporter:        Option[Reporter]
)
  extends Dispatcher[T] {
  val random = new Random(23)
  var droppingRate: DroppingRate = DroppingRate(0)
  protected lazy val queueProps = Queue.default(queueSampler, settings.workSettings)

  settings.regulator.foreach { rs ⇒
    context.actorOf(Regulator.props(rs, queueSampler, self), "regulator")
  }

  /**
   * This extraReceive implementation helps this PushingDispatcher act as a transparent proxy.  It will send the message to the underlying [[Queue]] and the
   * sender will be set as the receiver of any results of the downstream [[kanaloa.handler.Handler]].  This receive will disable any acks, and in the event of an [[EnqueueRejected]],
   * notify the original sender of the rejection.
   *
   * @return
   */
  override def extraReceive: Receive = {
    case EnqueueRejected(enqueued, reason) ⇒ enqueued.sendResultsTo.foreach(_ ! WorkRejected(reason.toString))
    case r: DroppingRate                   ⇒ droppingRate = r
    case m: T if classTag[T].runtimeClass.isInstance(m) ⇒ //todo: avoid this extra check when unnecessary (such as a method interface or the T is Any)

      queueSampler ! Metric.WorkReceived
      dropOrEnqueue(m, sender)
    case unrecognized ⇒ sender ! WorkFailed("unrecognized request")
  }

  private def dropOrEnqueue(m: Any, replyTo: ActorRef): Unit = {
    def drop(reason: String): Unit = {
      queueSampler ! Metric.WorkRejected
      sender ! WorkRejected(s"Request rejected by kanaloa dispatcher $name, $reason")
    }

    if (droppingRate.value > 0 &&
      (droppingRate.value == 1 || random.nextDouble() < droppingRate.value)) {
      drop(s"Over capacity or capacity is down, request is dropped under random dropping rate ${droppingRate.value}")
    } else if (workerPools.isEmpty && !inGracePeriod) {
      drop("The backend service is non longer working.")
    } else
      queue ! Enqueue(m, false, Some(replyTo))
  }
}

object PushingDispatcher {

  case object GracePeriodDone
  def props[T: ClassTag, A](
    name:            String,
    handlerProvider: A,
    rootConfig:      Config = ConfigFactory.load()
  )(implicit statsDClient: Option[StatsDClient], adaptor: HandlerProviderAdaptor[A, T]) = {
    val (settings, reporter) = Dispatcher.readConfig(name, rootConfig, statsDClient = statsDClient)
    Props(PushingDispatcher(name, settings, adaptor(handlerProvider), reporter)).withDeploy(Deploy.local)
  }
}

case class PullingDispatcher[T](
  name:            String,
  iterator:        Iterator[T],
  settings:        Settings,
  handlerProvider: HandlerProvider[T],
  reporter:        Option[Reporter],
  sendResultsTo:   Option[ActorRef]
) extends Dispatcher[T] {
  protected def queueProps = QueueOfIterator.props(
    iterator,
    settings.workSettings,
    queueSampler,
    sendResultsTo
  )
}

object PullingDispatcher {
  def props[T, A](
    name:            String,
    iterator:        Iterator[T],
    handlerProvider: A,
    sendResultsTo:   Option[ActorRef] = None,
    rootConfig:      Config           = ConfigFactory.load()
  )(
    implicit
    statsDClient: Option[StatsDClient],
    adaptor:      HandlerProviderAdaptor[A, T]
  ) = {
    val (settings, reporter) = Dispatcher.readConfig(name, rootConfig, statsDClient, Some("default-pulling-dispatcher"))
    //for pulling dispatchers because only a new idle worker triggers a pull of work, there maybe cases where there are two idle workers but the system should be deemed as fully utilized.
    Props(PullingDispatcher[T](name, iterator, settings, adaptor(handlerProvider), reporter, sendResultsTo)).withDeploy(Deploy.local)
  }
}
