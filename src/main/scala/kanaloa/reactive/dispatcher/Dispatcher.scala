package kanaloa.reactive.dispatcher

import akka.actor._
import com.typesafe.config.{ Config, ConfigFactory }
import kanaloa.reactive.dispatcher.ApiProtocol.{ ShutdownGracefully, WorkRejected }
import kanaloa.reactive.dispatcher.metrics.{ MetricsCollector, NoOpMetricsCollector }
import kanaloa.reactive.dispatcher.queue.Queue.EnqueueRejected.OverCapacity
import kanaloa.reactive.dispatcher.queue.Queue.{ Enqueue, EnqueueRejected, WorkEnqueued }
import kanaloa.reactive.dispatcher.queue._

import scala.concurrent.duration._

trait Dispatcher extends Actor {
  def name: String
  protected def dispatcherSettings: Dispatcher.Settings
  def backendProps: Props
  def resultChecker: ResultChecker
  def metricsCollector: MetricsCollector

  protected def queueProps: Props

  protected lazy val queue = context.actorOf(queueProps, name + "-backing-queue")

  private val processor = context.actorOf(QueueProcessor.withCircuitBreaker(
    queue,
    backendProps,
    dispatcherSettings.workerPool,
    dispatcherSettings.circuitBreaker,
    metricsCollector
  )(resultChecker), name + "-queue-processor")

  context watch processor

  private val autoScaler = dispatcherSettings.autoScalingSettings.foreach { s ⇒
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
    workTimeout:         FiniteDuration               = 1.minute,
    workRetry:           Int                          = 0,
    workerPool:          ProcessingWorkerPoolSettings,
    circuitBreaker:      CircuitBreakerSettings,
    autoScalingSettings: Option[AutoScalingSettings]
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

  val defaultDispatcherSettings = Dispatcher.Settings(
    workTimeout = 1.minute,
    workRetry = 0,
    workerPool = defaultWorkerPoolSettings,
    circuitBreaker = defaultCircuitBreakerSettings,
    autoScalingSettings = Some(defaultAutoScalingSettings)
  )
}

case class PushingDispatcher(
  name:             String,
  settings:         PushingDispatcher.Settings,
  backendProps:     Props,
  metricsCollector: MetricsCollector           = NoOpMetricsCollector,
  resultChecker:    ResultChecker
)
  extends Dispatcher {

  protected lazy val dispatcherSettings = settings.reactiveDispatcherSettings

  protected lazy val queueProps = Queue.withBackPressure(
    settings.backPressureSettings, WorkSettings(), metricsCollector
  )

  override def extraReceive: Receive = {
    case m ⇒ context.actorOf(PushingDispatcher.handlerProps(settings, queue)) forward m
  }
}

object PushingDispatcher {
  private class Handler(settings: Settings, queue: ActorRef) extends Actor with ActorLogging {
    def receive: Receive = {
      case msg ⇒
        queue ! Enqueue(msg, Some(self), Some(WorkSettings(settings.reactiveDispatcherSettings.workRetry, settings.reactiveDispatcherSettings.workTimeout, Some(sender))))
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
    name:          String,
    settings:      Settings,
    backendProps:  Props,
    metricsConfig: Config   = ConfigFactory.empty
  )(resultChecker: ResultChecker)(implicit system: ActorSystem) = {
    val metricsCollector = MetricsCollector.fromConfig(name, metricsConfig)
    Props(PushingDispatcher(name, settings, backendProps, metricsCollector, resultChecker))
  }

  val defaultBackPressureSettings = BackPressureSettings(
    maxBufferSize = 60000,
    thresholdForExpectedWaitTime = 1.minute,
    maxHistoryLength = 10.seconds
  )

  case class Settings(reactiveDispatcherSettings: Dispatcher.Settings, backPressureSettings: BackPressureSettings)

  val defaultSettings: Settings = Settings(Dispatcher.defaultDispatcherSettings, defaultBackPressureSettings)

}

case class PullingDispatcher(
  name:               String,
  iterator:           Iterator[_],
  dispatcherSettings: Dispatcher.Settings,
  backendProps:       Props,
  metricsCollector:   MetricsCollector    = NoOpMetricsCollector,
  resultChecker:      ResultChecker
) extends Dispatcher {

  protected def queueProps = QueueOfIterator.props(iterator, WorkSettings(), metricsCollector)

}

object PullingDispatcher {
  def props(
    name:          String,
    iterator:      Iterator[_],
    settings:      Dispatcher.Settings,
    backendProps:  Props,
    metricsConfig: Config              = ConfigFactory.empty
  )(resultChecker: ResultChecker)(implicit system: ActorSystem) = {
    val metricsCollector = MetricsCollector.fromConfig(name, metricsConfig)
    Props(PullingDispatcher(name, iterator, settings, backendProps, metricsCollector, resultChecker))
  }
}

