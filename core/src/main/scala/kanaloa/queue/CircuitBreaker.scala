package kanaloa.queue

import akka.actor.{Props, Actor, ActorLogging}
import kanaloa.metrics.Metric.WorkerPoolMetric
import kanaloa.metrics.{Metric, Reporter}
import kanaloa.queue.WorkerPoolManager.UnHold

class CircuitBreaker(
  workerPoolManagerRef: WorkerPoolManagerRef,
  settings:             CircuitBreakerSettings,
  reporter:             Option[Reporter]
) extends Actor with ActorLogging {
  override def receive: Receive = handlerResponding

  val handlerResponding: Receive = {
    case Metric.WorkTimedOut ⇒ context become handlerTimingOut(1)
    case e: WorkerPoolMetric ⇒ //ignore other metrics
  }

  def handlerTimingOut(count: Int): Receive = {
    case Metric.WorkCompleted(_) | Metric.WorkFailed ⇒
      workerPoolManagerRef ! UnHold
      reporter.foreach(_.report(Metric.CircuitBreakerClosed))
      context become handlerResponding

    case Metric.WorkTimedOut ⇒
      val newCount = count + 1
      if (newCount <= settings.timeoutCountThreshold) {
        context become handlerTimingOut(newCount)
      } else {
        val openFor = settings.openDurationBase * Math.min(newCount - settings.timeoutCountThreshold, settings.maxOpenFactor).toLong
        workerPoolManagerRef ! kanaloa.handler.Hold(openFor)
        reporter.foreach(_.report(Metric.CircuitBreakerOpened))
      }

    case e: WorkerPoolMetric ⇒ //ignore other metrics
  }
}

object CircuitBreaker {
  def props(
    workerPoolManagerRef: WorkerPoolManagerRef,
    settings:             CircuitBreakerSettings,
    reporter:             Option[Reporter]
  ): Props = Props(new CircuitBreaker(workerPoolManagerRef, settings, reporter))
}
