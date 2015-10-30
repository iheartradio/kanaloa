package kanaloa.reactive.dispatcher

import akka.actor._
import com.typesafe.config.{ Config, ConfigFactory }
import kanaloa.reactive.dispatcher.ApiProtocol.{ ShutdownGracefully, WorkRejected }
import kanaloa.reactive.dispatcher.Dispatcher.Settings
import kanaloa.reactive.dispatcher.metrics.{ StatsDMetricsCollectorSettings, MetricsCollectorSettings, MetricsCollector, NoOpMetricsCollector }
import kanaloa.reactive.dispatcher.queue.Queue.EnqueueRejected.OverCapacity
import kanaloa.reactive.dispatcher.queue.Queue.{ Enqueue, EnqueueRejected, WorkEnqueued }
import kanaloa.reactive.dispatcher.queue._

import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._

import scala.concurrent.duration._

trait Dispatcher extends Actor {
  def name: String
  def settings: Dispatcher.Settings
  def backendProps: Props
  def resultChecker: ResultChecker
  def metricsCollector: MetricsCollector

  protected def queueProps: Props

  protected lazy val queue = context.actorOf(queueProps, name + "-backing-queue")

  private val processor = context.actorOf(QueueProcessor.withCircuitBreaker(
    queue,
    backendProps,
    settings.workerPool,
    settings.circuitBreaker,
    metricsCollector
  )(resultChecker), name + "-queue-processor")

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
    workTimeout:    FiniteDuration               = 1.minute,
    workRetry:      Int                          = 0,
    workerPool:     ProcessingWorkerPoolSettings,
    circuitBreaker: CircuitBreakerSettings,
    backPressure:   Option[BackPressureSettings],
    autoScaling:    Option[AutoScalingSettings]
  )

  val defaultCircuitBreakerSettings = CircuitBreakerSettings(
    closeDuration = 3.seconds,
    errorRateThreshold = 1,
    historyLength = 3
  )

  val defaultWorkerPoolSettings = ProcessingWorkerPoolSettings(
    startingPoolSize = 20,
    maxProcessingTime = None,
    minPoolSize = 5
  )

  val defaultAutoScalingSettings = AutoScalingSettings(
    bufferRatio = 0.8
  )

  val defaultBackPressureSettings = BackPressureSettings(
    maxBufferSize = 60000,
    thresholdForExpectedWaitTime = 1.minute,
    maxHistoryLength = 10.seconds
  )

  val defaultDispatcherSettings = Dispatcher.Settings(
    workTimeout = 1.minute,
    workRetry = 0,
    workerPool = defaultWorkerPoolSettings,
    circuitBreaker = defaultCircuitBreakerSettings,
    backPressure = None,
    autoScaling = Some(defaultAutoScalingSettings)
  )

  def readConfig(dispatcherName: String, rootConfig: Config)(implicit system: ActorSystem): (Settings, MetricsCollector) = {
    val config = rootConfig.as[Option[Config]]("kanaloa").getOrElse(ConfigFactory.empty())
    (
      config.as[Option[Dispatcher.Settings]]("dispatchers." + dispatcherName).getOrElse(defaultDispatcherSettings),
      MetricsCollector.fromConfig(dispatcherName, config)
    )
  }
}

case class PushingDispatcher(
  name:             String,
  settings:         Settings,
  backendProps:     Props,
  metricsCollector: MetricsCollector = NoOpMetricsCollector,
  resultChecker:    ResultChecker
)
  extends Dispatcher {

  protected lazy val queueProps = Queue.withBackPressure(
    settings.backPressure.getOrElse(Dispatcher.defaultBackPressureSettings), WorkSettings(), metricsCollector
  )

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

  def props(
    name:         String,
    backendProps: Props,
    rootConfig:   Config = ConfigFactory.load()
  )(resultChecker: ResultChecker)(implicit system: ActorSystem) = {
    val (settings, metricsCollector) = Dispatcher.readConfig(name, rootConfig)
    Props(PushingDispatcher(name, settings, backendProps, metricsCollector, resultChecker))
  }

}

case class PullingDispatcher(
  name:             String,
  iterator:         Iterator[_],
  settings:         Settings,
  backendProps:     Props,
  metricsCollector: MetricsCollector = NoOpMetricsCollector,
  resultChecker:    ResultChecker
) extends Dispatcher {
  protected def queueProps = QueueOfIterator.props(iterator, WorkSettings(settings.workRetry, settings.workTimeout), metricsCollector)
}

object PullingDispatcher {
  def props(
    name:         String,
    iterator:     Iterator[_],
    backendProps: Props,
    rootConfig:   Config      = ConfigFactory.load()
  )(resultChecker: ResultChecker)(implicit system: ActorSystem) = {
    val (settings, metricsCollector) = Dispatcher.readConfig(name, rootConfig)
    Props(PullingDispatcher(name, iterator, settings, backendProps, metricsCollector, resultChecker))
  }
}

