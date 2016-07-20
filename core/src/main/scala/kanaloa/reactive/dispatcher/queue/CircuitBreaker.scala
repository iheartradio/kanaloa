package kanaloa.reactive.dispatcher.queue

import akka.actor._
import kanaloa.reactive.dispatcher.ApiProtocol.{WorkFailed, WorkTimedOut}
import kanaloa.reactive.dispatcher.queue.QueueProcessor.WorkCompleted
import kanaloa.reactive.dispatcher.queue.Worker.SetDelay

class CircuitBreaker(
  settings: CircuitBreakerSettings,
  worker:   WorkerRef
) extends Actor with ActorLogging {

  val receive: Receive = monitoring(0)

  def monitoring(timeoutCount: Int): Receive = {

    case WorkCompleted(_, _) | WorkFailed(_) ⇒
      worker ! SetDelay(None)
      context become receive

    case WorkTimedOut(_) ⇒
      val newCount = timeoutCount + 1
      context become monitoring(newCount)
      if (newCount >= settings.timeoutCountThreshold)
        worker ! SetDelay(Some(settings.openDurationBase * newCount))

  }
}

object CircuitBreaker {
  def props(settings: CircuitBreakerSettings, workerRef: WorkerRef) =
    Props(new CircuitBreaker(settings, workerRef))
}
