package kanaloa

import akka.actor.SupervisorStrategy.Escalate
import akka.actor._
import com.typesafe.config.{Config, ConfigFactory}
import kanaloa.ApiProtocol.{WorkFailed, ShutdownGracefully, WorkRejected}
import kanaloa.Dispatcher.{UnSubscribePerformanceMetrics, SubscribePerformanceMetrics, Settings}
import kanaloa.Regulator.DroppingRate
import kanaloa.handler.{HandlerProviderAdaptor, HandlerProvider}
import kanaloa.metrics.{StatsDClient, Metric, MetricsCollector, Reporter}
import kanaloa.queue.Queue.{Enqueue, EnqueueRejected}
import kanaloa.queue._
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.readers.namemappers.implicits.hyphenCase
import net.ceedubs.ficus.readers.ValueReader

import scala.concurrent.duration._
import scala.reflect._
import scala.util.Random

trait Dispatcher[T] extends Actor with ActorLogging {
  def name: String
  def settings: Dispatcher.Settings
  def handlerProvider: HandlerProvider[T]
  def reporter: Option[Reporter]

  protected lazy val queueSampler = context.actorOf(QueueSampler.props(reporter, settings.samplerSettings), "queueSampler")
  //todo: move this into the QueueProcessor
  protected lazy val workerPoolSampler = context.actorOf(WorkerPoolSampler.props(reporter, queueSampler, settings.samplerSettings))

  protected def queueProps: Props

  protected lazy val queue = context.actorOf(queueProps, "queue")

  override val supervisorStrategy =
    OneForOneStrategy() {
      case e ⇒
        log.error(s"Dispatcher $name one of the children actor died due to $e")
        Escalate
    }

  private[kanaloa] val processor = {
    val props = QueueProcessor.default(
      queue,
      handlerProvider,
      settings.workerPool,
      workerPoolSampler,
      settings.circuitBreaker
    )

    context.actorOf(props, "queue-processor")
  }

  context watch processor

  private val _ = settings.autothrottle.foreach { s ⇒
    context.actorOf(Autothrottler.default(processor, s, workerPoolSampler), "autothrottler")
  }

  def receive: Receive = ({
    case ShutdownGracefully(reportBack, timeout) ⇒ processor ! QueueProcessor.Shutdown(reportBack, timeout)
    case SubscribePerformanceMetrics(actor)      ⇒ queueSampler ! Sampler.Subscribe(actor)
    case UnSubscribePerformanceMetrics(actor)    ⇒ queueSampler ! Sampler.Unsubscribe(actor)
    case Terminated(`processor`) ⇒
      context stop self
      log.info(s"Dispatcher $name is shutdown")
  }: Receive) orElse extraReceive

  def extraReceive: Receive = PartialFunction.empty
}

object Dispatcher {

  private[kanaloa] case class SubscribePerformanceMetrics(actor: ActorRef)
  private[kanaloa] case class UnSubscribePerformanceMetrics(actor: ActorRef)

  case class Settings(
    workTimeout:               FiniteDuration                 = 1.minute,
    workRetry:                 Int                            = 0,
    updateInterval:            FiniteDuration                 = 1.second,
    lengthOfDisplayForMessage: Int                            = 200,
    workerPool:                ProcessingWorkerPoolSettings,
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

      workerPoolSampler ! Metric.WorkReceived
      dropOrEnqueue(m, sender)
    case unrecognized ⇒ sender ! WorkFailed("unrecognized request")
  }

  private def dropOrEnqueue(m: Any, replyTo: ActorRef): Unit = {
    if (droppingRate.value > 0 &&
      (droppingRate.value == 1 || random.nextDouble() < droppingRate.value)) {
      workerPoolSampler ! Metric.WorkRejected
      sender ! WorkRejected(s"Over capacity or capacity is down, request is dropped under random dropping rate ${droppingRate.value} by kanaloa dispatcher $name")
    } else
      queue ! Enqueue(m, false, Some(replyTo))
  }
}

object PushingDispatcher {
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
