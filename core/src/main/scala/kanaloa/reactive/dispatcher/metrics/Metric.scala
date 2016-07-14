package kanaloa.reactive.dispatcher.metrics

import scala.concurrent.duration._

sealed trait Metric

object Metric {
  sealed trait Event extends Metric

  case object WorkEnqueued extends Event
  case object EnqueueRejected extends Event

  case object WorkTimedOut extends Event
  case object WorkFailed extends Event

  case object CircuitBreakerOpened extends Event

  sealed trait Status extends Metric

  case class PoolSize(size: Int) extends Status
  case class PoolUtilized(numWorkers: Int) extends Status
  case class WorkQueueLength(length: Int) extends Status
  case class WorkQueueExpectedWaitTime(duration: Duration) extends Status

  case class WorkCompleted(processTime: Duration) extends Event with Status
}

