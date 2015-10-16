package com.iheart.workpipeline.metrics

import scala.concurrent.duration._

sealed trait Metric

/** Events, usually used to increment/decrement a counter */
object Event {
  case object WorkEnqueued extends Metric
  case object WorkDequeued extends Metric
  case object EnqueueRejected extends Metric

  case object WorkCompleted extends Metric
  case object WorkTimedOut extends Metric
  case object WorkFailed extends Metric
}

/** Statuses describing the state of the system at any given time */
object Status {
  case class PoolSize(size: Int, utilized: Int) extends Metric
  case class AverageWaitTime(duration: Duration) extends Metric
  case class WorkQueueLength(length: Int) extends Metric
  case class WorkQueueMaxLength(length: Int) extends Metric
}

