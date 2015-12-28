package kanaloa.reactive.dispatcher

import akka.actor._
import com.typesafe.config.{Config, ConfigFactory}
import kanaloa.reactive.dispatcher.ApiProtocol.{ShutdownGracefully, WorkRejected}
import kanaloa.reactive.dispatcher.Backend.BackendAdaptor
import kanaloa.reactive.dispatcher.Dispatcher.Settings
import kanaloa.reactive.dispatcher.metrics.{MetricsCollector, NoOpMetricsCollector}
import kanaloa.reactive.dispatcher.queue.Queue.EnqueueRejected.OverCapacity
import kanaloa.reactive.dispatcher.queue.Queue.{Enqueue, EnqueueRejected, WorkEnqueued}
import kanaloa.reactive.dispatcher.queue._
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.readers.ValueReader

import scala.concurrent.duration._

trait Dispatcher extends Actor {
  def name: String
  def settings: Dispatcher.Settings
  def backend: Backend
  def resultChecker: ResultChecker
  def metricsCollector: MetricsCollector

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
    context.actorOf(AutoScaling.default(queue, processor, s, metricsCollector), name + "-auto-scaler")
  }

  def receive: Receive = ({
    case ShutdownGracefully(reportBack, timeout) ⇒ processor ! QueueProcessor.Shutdown(reportBack, timeout, true)
    case Terminated(`processor`)                 ⇒ context stop self
  }: Receive) orElse extraReceive

  def extraReceive: Receive = PartialFunction.empty
}

object Dispatcher {

  case class Settings(
    workTimeout:     FiniteDuration                 = 1.minute,
    workRetry:       Int                            = 0,
    dispatchHistory: DispatchHistorySettings,
    workerPool:      ProcessingWorkerPoolSettings,
    circuitBreaker:  Option[CircuitBreakerSettings],
    backPressure:    Option[BackPressureSettings],
    autoScaling:     Option[AutoScalingSettings]
  )

  val defaultBackPressureSettings = BackPressureSettings(
    maxBufferSize = 60000,
    thresholdForExpectedWaitTime = 5.minute,
    maxHistoryLength = 10.seconds
  )

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
      backPressure = readComponent[BackPressureSettings]("backPressure", config),
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

  def readConfig(dispatcherName: String, rootConfig: Config)(implicit system: ActorSystem): (Settings, MetricsCollector) = {
    val cfg = kanaloaConfig(rootConfig)
    val dispatcherCfg = cfg.as[Option[Config]]("dispatchers." + dispatcherName).getOrElse(ConfigFactory.empty).withFallback(defaultDispatcherConfig(cfg))

    val settings = toDispatcherSettings(dispatcherCfg)

    val metricsCollector = MetricsCollector.fromConfig(dispatcherName, cfg)

    (settings, metricsCollector)
  }

}

case class PushingDispatcher(
  name:             String,
  settings:         Settings,
  backend:          Backend,
  metricsCollector: MetricsCollector = NoOpMetricsCollector,
  resultChecker:    ResultChecker
)
  extends Dispatcher {

  protected lazy val queueProps = settings.backPressure match {
    case Some(bp) ⇒ Queue.withBackPressure(settings.dispatchHistory, bp, WorkSettings(), metricsCollector)
    case None     ⇒ Queue.default(settings.dispatchHistory, WorkSettings(), metricsCollector)
  }

  override def extraReceive: Receive = {
    case m ⇒ context.actorOf(PushingDispatcher.handlerProps(settings, queue)) forward m
  }
}

object PushingDispatcher {
  private class Handler(settings: Settings, queue: ActorRef) extends Actor with ActorLogging {
    def receive: Receive = {
      case msg ⇒
        queue ! Enqueue(msg, Some(self), Some(WorkSettings(settings.workRetry, settings.workTimeout, Some(sender))))
        context become waitingForQueueConfirmation(sender)
    }

    def waitingForQueueConfirmation(replyTo: ActorRef): Receive = {
      case WorkEnqueued ⇒
        context stop self //mission accomplished
      case EnqueueRejected(_, OverCapacity) ⇒
        replyTo ! WorkRejected("Server out of capacity")
        context stop self
      case m ⇒
        replyTo ! WorkRejected(s"unexpected response $m")
        context stop self
    }
  }

  private def handlerProps(settings: Settings, queue: QueueRef) = {
    Props(new Handler(settings, queue))
  }

  def props[T: BackendAdaptor](
    name:       String,
    backend:    T,
    rootConfig: Config = ConfigFactory.load()
  )(resultChecker: ResultChecker)(implicit system: ActorSystem) = {
    val (settings, metricsCollector) = Dispatcher.readConfig(name, rootConfig)
    Props(PushingDispatcher(name, settings, backend, metricsCollector, resultChecker))
  }

}

case class PullingDispatcher(
  name:             String,
  iterator:         Iterator[_],
  settings:         Settings,
  backend:          Backend,
  metricsCollector: MetricsCollector = NoOpMetricsCollector,
  resultChecker:    ResultChecker
) extends Dispatcher {
  protected def queueProps = QueueOfIterator.props(iterator, settings.dispatchHistory, WorkSettings(settings.workRetry, settings.workTimeout), metricsCollector)
}

object PullingDispatcher {
  def props[T: BackendAdaptor](
    name:       String,
    iterator:   Iterator[_],
    backend:    T,
    rootConfig: Config      = ConfigFactory.load()
  )(resultChecker: ResultChecker)(implicit system: ActorSystem) = {
    val (settings, metricsCollector) = Dispatcher.readConfig(name, rootConfig)
    Props(PullingDispatcher(name, iterator, settings, backend, metricsCollector, resultChecker))
  }
}

