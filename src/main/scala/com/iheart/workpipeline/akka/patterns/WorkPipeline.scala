package com.iheart.workpipeline.akka.patterns

import akka.actor._
import ActorDSL._
import com.iheart.workpipeline.akka.patterns.CommonProtocol.ShutdownGracefully
import com.iheart.workpipeline.akka.patterns.WorkPipeline.Settings
import com.iheart.workpipeline.akka.patterns.queue._
import queue.CommonProtocol.WorkRejected
import queue.Queue.{EnqueueRejected, WorkEnqueued, Enqueue}
import EnqueueRejected.OverCapacity

import scala.concurrent.duration._

trait WorkPipeline extends Actor {
  def name: String
  protected def pipelineSettings: WorkPipeline.Settings
  def backendProps: Props
  def resultChecker: ResultChecker

  protected def queueProps: Props

  protected lazy val queue = context.actorOf(queueProps, name + "-backing-queue")

  private val processor = context.actorOf(QueueProcessor.withCircuitBreaker(queue,
    backendProps,
    pipelineSettings.workerPool,
    pipelineSettings.circuitBreaker)(resultChecker), name + "-queue-processor")

  context watch processor

  private val autoScaler =  pipelineSettings.autoScalingSettings.foreach { s =>
    context.actorOf(AutoScaling.default(queue, processor, s), name + "-auto-scaler" )
  }

  def receive = ({
    case ShutdownGracefully(reportBack, timeout) ⇒ processor ! QueueProcessor.Shutdown(reportBack, timeout, true)
    case Terminated(`processor`) ⇒ context stop self
  }: Receive) orElse extraReceive

  def extraReceive: Receive = PartialFunction.empty
}

object WorkPipeline {
  case class Settings( workTimeout: FiniteDuration = 1.minute,
                       workRetry: Int = 0,
                       workerPool: ProcessingWorkerPoolSettings,
                       circuitBreaker: CircuitBreakerSettings,
                       autoScalingSettings: Option[AutoScalingSettings])

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

  val defaultWorkPipelineSettings = WorkPipeline.Settings(
    workTimeout = 1.minute,
    workRetry = 0,
    workerPool = defaultWorkerPoolSettings,
    circuitBreaker = defaultCircuitBreakerSettings,
    autoScalingSettings = Some(defaultAutoScalingSettings))
}

case class PushingWorkPipeline(name: String,
                   settings: PushingWorkPipeline.Settings,
                   backendProps: Props,
                   resultChecker: ResultChecker)
  extends WorkPipeline {

  protected val pipelineSettings = settings.workPipeLineSettings

  protected val queueProps = Queue.withBackPressure(settings.backPressureSettings, WorkSettings())

  override def extraReceive: Receive = {
    case m ⇒ context.actorOf(PushingWorkPipeline.handlerProps(settings, queue)) forward m
  }
}

object PushingWorkPipeline {
  private class Handler(settings: Settings, queue: ActorRef) extends Actor with ActorLogging {
    def receive: Receive = {
      case msg =>
        queue ! Enqueue(msg, Some(self), Some(WorkSettings(settings.workPipeLineSettings.workRetry, settings.workPipeLineSettings.workTimeout, Some(sender))))
        context become waitingForQueueConfirmation(sender)
    }

    def waitingForQueueConfirmation(replyTo: ActorRef): Receive = {
      case WorkEnqueued =>
        context stop self //mission accomplished
      case EnqueueRejected(_, OverCapacity) =>
        replyTo ! WorkRejected("Server out of capacity")
        context stop self
      case m  =>
        replyTo ! WorkRejected(s"unexpected response $m")
        context stop self
    }
  }

  private def handlerProps(settings: Settings, queue: QueueRef) = {
    Props(new Handler(settings, queue))
  }

  def props(name: String, settings: Settings, backendProps: Props)
           (resultChecker: ResultChecker) = Props(PushingWorkPipeline(name, settings, backendProps, resultChecker))

  val defaultBackPressureSettings = BackPressureSettings(
    maxBufferSize = 60000,
    thresholdForExpectedWaitTime = 1.minute,
    maxHistoryLength = 10.seconds)



  case class Settings(workPipeLineSettings: WorkPipeline.Settings, backPressureSettings: BackPressureSettings)

  val defaultSettings: Settings = Settings(WorkPipeline.defaultWorkPipelineSettings, defaultBackPressureSettings)


}

case class PullingWorkPipeline( name: String,
                                iterator: Iterator[_],
                                pipelineSettings: WorkPipeline.Settings,
                                backendProps: Props,
                                resultChecker: ResultChecker) extends WorkPipeline {

  protected def queueProps = QueueOfIterator.props(iterator, WorkSettings())

}

object PullingWorkPipeline {
  def props(name: String, iterator: Iterator[_], settings: WorkPipeline.Settings, backendProps: Props)
           (resultChecker: ResultChecker) = Props(PullingWorkPipeline(name, iterator, settings, backendProps, resultChecker))
}

