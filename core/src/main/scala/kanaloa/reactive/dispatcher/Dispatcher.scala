package kanaloa.reactive.dispatcher

import akka.actor._
import com.typesafe.config.{Config, ConfigFactory}
import kanaloa.reactive.dispatcher.ApiProtocol.{ShutdownGracefully, WorkRejected}
import kanaloa.reactive.dispatcher.Backend.BackendAdaptor
import kanaloa.reactive.dispatcher.Dispatcher.Settings
import kanaloa.reactive.dispatcher.Regulator.DroppingRate
import kanaloa.reactive.dispatcher.metrics.{Metric, MetricsCollector, Reporter}
import kanaloa.reactive.dispatcher.queue.Queue.{Enqueue, EnqueueRejected}
import kanaloa.reactive.dispatcher.queue._
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.readers.ValueReader

import scala.concurrent.duration._
import scala.util.Random

trait Dispatcher extends Actor {
  def name: String
  def settings: Dispatcher.Settings
  def backend: Backend
  def resultChecker: ResultChecker
  def metricsCollector: ActorRef

  protected def queueProps: Props

  protected lazy val queue = context.actorOf(queueProps, name + "-backing-queue")

  private[dispatcher] val processor = {
    val props = (settings.circuitBreaker match {
      case Some(cb) ⇒
        QueueProcessor.withCircuitBreaker(queue, backend, settings.workerPool, cb, metricsCollector) _
      case None ⇒
        QueueProcessor.default(queue, backend, settings.workerPool, metricsCollector) _
    })(resultChecker)

    context.actorOf(props, name + "-queue-processor")
  }

  context watch processor

  private val autoScaler = settings.autoScaling.foreach { s ⇒
    context.actorOf(AutoScaling.default(processor, s, metricsCollector), name + "-auto-scaler")
  }

  def receive: Receive = ({
    case ShutdownGracefully(reportBack, timeout) ⇒ processor ! QueueProcessor.Shutdown(reportBack, timeout)
    case Terminated(`processor`)                 ⇒ context stop self
  }: Receive) orElse extraReceive

  def extraReceive: Receive = PartialFunction.empty
}

object Dispatcher {

  case class Settings(
    workTimeout:    FiniteDuration                 = 1.minute,
    workRetry:      Int                            = 0,
    updateInterval: FiniteDuration                 = 1.second,
    workerPool:     ProcessingWorkerPoolSettings,
    regulator:      Option[Regulator.Settings],
    circuitBreaker: Option[CircuitBreakerSettings],
    autoScaling:    Option[AutoScalingSettings]
  ) {
    val performanceSamplerSettings = PerformanceSampler.PerformanceSamplerSettings(updateInterval)
  }

  private[dispatcher] def kanaloaConfig(rootConfig: Config = ConfigFactory.empty) = {
    val referenceConfig = ConfigFactory.defaultReference(getClass.getClassLoader).getConfig("kanaloa")

    rootConfig.as[Option[Config]]("kanaloa")
      .getOrElse(ConfigFactory.empty())
      .withFallback(referenceConfig)
  }

  def defaultDispatcherConfig(config: Config = kanaloaConfig()): Config =
    config.as[Config]("default-dispatcher")

  def defaultDispatcherSettings(config: Config = kanaloaConfig()): Dispatcher.Settings =
    toDispatcherSettings(defaultDispatcherConfig(config))

  private def toDispatcherSettings(config: Config): Dispatcher.Settings = {
    val settings = config.atPath("root").as[Dispatcher.Settings]("root")
    settings.copy(
      regulator = readComponent[Regulator.Settings]("backPressure", config),
      circuitBreaker = readComponent[CircuitBreakerSettings]("circuitBreaker", config),
      autoScaling = readComponent[AutoScalingSettings]("autoScaling", config)
    )
  }

  private def readComponent[SettingT: ValueReader](name: String, config: Config): Option[SettingT] =
    for {
      componentCfg ← config.as[Option[Config]](name)
      enabled ← componentCfg.as[Option[Boolean]]("enabled")
      settings ← componentCfg.atPath("root").as[Option[SettingT]]("root") if enabled
    } yield settings

  def readConfig(dispatcherName: String, rootConfig: Config)(implicit system: ActorSystem): (Settings, Option[Reporter]) = {
    val cfg = kanaloaConfig(rootConfig)
    val dispatcherCfg = cfg.as[Option[Config]]("dispatchers." + dispatcherName).getOrElse(ConfigFactory.empty).withFallback(defaultDispatcherConfig(cfg))

    val settings = toDispatcherSettings(dispatcherCfg)

    (settings, Reporter.fromConfig(dispatcherName: String, dispatcherCfg))
  }

}

case class PushingDispatcher(
  name:             String,
  settings:         Settings,
  backend:          Backend,
  metricsCollector: ActorRef,
  resultChecker:    ResultChecker
)
  extends Dispatcher {
  val random = new Random(23)
  var droppingRate: DroppingRate = DroppingRate(0)
  protected lazy val queueProps = Queue.default(metricsCollector, WorkSettings(settings.workRetry, settings.workTimeout))

  settings.regulator.foreach { rs ⇒
    context.actorOf(Regulator.props(rs, metricsCollector, self))
  }

  /**
   * This extraReceive implementation helps this PushingDispatcher act as a transparent proxy.  It will send the message to the underlying [[Queue]] and the
   * sender will be set as the receiver of any results of the downstream [[Backend]].  This receive will disable any acks, and in the event of an [[EnqueueRejected]],
   * notify the original sender of the rejection.
   *
   * @return
   */
  override def extraReceive: Receive = {
    case EnqueueRejected(enqueued, reason) ⇒ enqueued.sendResultsTo.foreach(_ ! WorkRejected(reason.toString))
    case r: DroppingRate                   ⇒ droppingRate = r
    case m                                 ⇒ dropOrEnqueue(m, sender)
  }

  private def dropOrEnqueue(m: Any, replyTo: ActorRef): Unit = {
    if (droppingRate.value > 0 &&
      (droppingRate.value == 1 || random.nextDouble() < droppingRate.value)) {
      metricsCollector ! Metric.WorkRejected
      sender ! WorkRejected(s"Over capacity, request dropped under random dropping rate ${droppingRate.value}")
    } else
      queue ! Enqueue(m, false, Some(replyTo))
  }
}

object PushingDispatcher {

  def props[T: BackendAdaptor](
    name:       String,
    backend:    T,
    rootConfig: Config = ConfigFactory.load()
  )(resultChecker: ResultChecker)(implicit system: ActorSystem) = {
    val (settings, reporter) = Dispatcher.readConfig(name, rootConfig)
    val metricsCollector = MetricsCollector(reporter, settings.performanceSamplerSettings)
    val toBackend = implicitly[BackendAdaptor[T]]
    Props(PushingDispatcher(name, settings, toBackend(backend), metricsCollector, resultChecker)).withDeploy(Deploy.local)
  }
}

case class PullingDispatcher(
  name:             String,
  iterator:         Iterator[_],
  settings:         Settings,
  backend:          Backend,
  metricsCollector: ActorRef,
  sendResultsTo:    Option[ActorRef],
  resultChecker:    ResultChecker
) extends Dispatcher {
  protected def queueProps = QueueOfIterator.props(
    iterator,
    WorkSettings(settings.workRetry, settings.workTimeout),
    metricsCollector,
    sendResultsTo
  )
}

object PullingDispatcher {
  def props[T: BackendAdaptor](
    name:          String,
    iterator:      Iterator[_],
    backend:       T,
    sendResultsTo: Option[ActorRef],
    rootConfig:    Config           = ConfigFactory.load()
  )(resultChecker: ResultChecker)(implicit system: ActorSystem) = {
    val (settings, reporter) = Dispatcher.readConfig(name, rootConfig)
    //for pulling dispatchers because only a new idle worker triggers a pull of work, there maybe cases where there are two idle workers but the system should be deemed as fully utilized.
    val metricsCollector = MetricsCollector(reporter, settings.performanceSamplerSettings.copy(fullyUtilized = _ <= 2))
    val toBackend = implicitly[BackendAdaptor[T]]
    Props(PullingDispatcher(name, iterator, settings, toBackend(backend), metricsCollector, sendResultsTo, resultChecker)).withDeploy(Deploy.local)
  }
}

