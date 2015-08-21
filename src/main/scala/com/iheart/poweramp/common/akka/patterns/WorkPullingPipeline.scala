package com.iheart.poweramp.common.akka.patterns

import akka.actor._
import com.iheart.poweramp.common.akka.patterns.WorkPullingPipeline.Settings

import com.iheart.poweramp.common.akka.patterns.queue._
import queue.CommonProtocol.WorkRejected
import queue.Queue.{EnqueueRejected, WorkEnqueued, Enqueue}
import EnqueueRejected.OverCapacity

import scala.concurrent.duration._

class WorkPullingPipeline(name: String, settings: Settings, backendProps: Props)
                         (resultChecker: ResultChecker)
                         (implicit system: ActorSystem) {

  private class WorkPullingHandler(settings: Settings, queue: ActorRef) extends Actor with ActorLogging {
    def receive: Receive = {
      case msg =>
        queue ! Enqueue(msg, Some(self), Some(WorkSettings(settings.workRetry, settings.workTimeout, Some(sender))))
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

  private val queue = system.actorOf(Queue.withBackPressure(settings.backPressure, WorkSettings()), name + "-backing-queue")

  private val processor = system.actorOf(QueueProcessor.withCircuitBreaker(queue,
                                              backendProps,
                                              settings.workerPool,
                                              settings.circuitBreaker)(resultChecker), name + "-queue-processor")

  private val autoScaler =  settings.autoScalingSettings.foreach { s =>
    system.actorOf(AutoScaling.default(queue, processor, s), name + "-auto-scaler" )
  }


  def handlerProps = {
    Props(new WorkPullingHandler(settings, queue))
  }
}

object WorkPullingPipeline {

  val defaultBackPressureSettings = BackPressureSettings(
    maxBufferSize = 60000,
    thresholdForExpectedWaitTime = 1.minute,
    maxHistoryLength = 10.seconds)

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

  )

  case class Settings( workTimeout: FiniteDuration = 1.minute,
                       workRetry: Int = 0,
                       workerPool: ProcessingWorkerPoolSettings = defaultWorkerPoolSettings,
                       backPressure: BackPressureSettings = defaultBackPressureSettings,
                       circuitBreaker: CircuitBreakerSettings = defaultCircuitBreakerSettings,
                       autoScalingSettings: Option[AutoScalingSettings] = Some(defaultAutoScalingSettings))

}
