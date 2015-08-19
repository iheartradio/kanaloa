package com.iheart.poweramp.common.akka.patterns

import akka.actor._
import com.iheart.poweramp.common.akka.patterns.WorkPullingPipeline.{Rejected, Settings}
import com.iheart.poweramp.common.akka.patterns.queue.{AutoScaling, QueueProcessor, Queue}
import com.iheart.poweramp.common.akka.patterns.queue.Queue.EnqueueRejected.OverCapacity
import com.iheart.poweramp.common.akka.patterns.queue.Queue.{BackPressureSettings, EnqueueRejected, WorkAdded, Enqueue}
import com.iheart.poweramp.common.akka.patterns.queue.Worker._

import scala.concurrent.duration._

class WorkPullingPipeline(name: String, settings: Settings, backendProps: Props)
                         (resultChecker: ResultChecker)
                         (implicit system: ActorSystem) {

  private class WorkPullingHandler(settings: Settings, queue: ActorRef) extends Actor with ActorLogging {
    def receive: Receive = {
      case msg =>
        queue ! Enqueue(msg, Some(self), Some(WorkSetting(settings.workRetry, settings.workTimeout, Some(sender))))
        context become waitingForQueueConfirmation(sender)
    }

    def waitingForQueueConfirmation(replyTo: ActorRef): Receive = {
      case WorkAdded =>
        context stop self //mission accomplished
      case EnqueueRejected(_, OverCapacity) =>
        replyTo ! Rejected("Server out of capacity")
        context stop self
      case m  =>
        replyTo ! Rejected(s"unexpected response $m")
        context stop self
    }
  }

  private lazy val queue = system.actorOf(Queue.withBackPressure(settings.backPressure, WorkSetting()), name + "-backing-queue")

  private lazy val processor = system.actorOf(QueueProcessor.withCircuitBreaker(queue,
                                              backendProps,
                                              QueueProcessor.Settings(settings.numberOfParallelWorkers),
                                              settings.circuitBreaker)(resultChecker), name + "-queue-processor")

  private lazy val autoScaler = system.actorOf(AutoScaling.default(queue, processor), name + "-auto-scaler" )


  def handlerProps = {
    autoScaler //lazy initialized system actors
    Props(new WorkPullingHandler(settings, queue))
  }
}

object WorkPullingPipeline {

  case class Rejected(reason: String)

  val defaultBackPressureSettings: BackPressureSettings = BackPressureSettings(
    maxBufferSize = 60000,  //a good approximation is (thresholdForExpectedWaitTime / expected normal processing time), in this case the expected normal processing time is 1 ms
    thresholdForExpectedWaitTime = 1.minute,
    maxHistoryLength = 10.seconds)

  val defaultCircuitBreakerSettings: CircuitBreakerSettings = CircuitBreakerSettings(
    closeDuration = 3.seconds,
    errorRateThreshold = 1,
    historyLength = 5
  )

  case class Settings( numberOfParallelWorkers: Int = 20, //relatively high number given our control
                       workTimeout: FiniteDuration = 1.minute,
                       workRetry: Int = 0,
                       backPressure: BackPressureSettings = defaultBackPressureSettings,

                       circuitBreaker: CircuitBreakerSettings = defaultCircuitBreakerSettings)

}
