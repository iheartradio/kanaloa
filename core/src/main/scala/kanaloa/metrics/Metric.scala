package kanaloa.metrics

import scala.concurrent.duration._

sealed trait Metric

object Metric {

  trait Event extends Metric
  sealed trait Status extends Metric

  trait QueueMetric extends Metric

  trait WorkerPoolMetric extends Metric

  case object WorkReceived extends Event with QueueMetric

  case object WorkRejected extends Event with QueueMetric

  case object WorkShedded extends Event with QueueMetric

  case class DropRate(value: Double) extends Status with QueueMetric
  case class BurstMode(inBurst: Boolean) extends Status with QueueMetric
  case class WorkQueueLength(length: Int) extends Status with QueueMetric
  case class WorkQueueExpectedWaitTime(duration: Duration) extends Status with QueueMetric

  case object WorkTimedOut extends Event with WorkerPoolMetric

  case object WorkFailed extends Event with WorkerPoolMetric
  case class WorkCompleted(processTime: Duration) extends Event with WorkerPoolMetric
  case object CircuitBreakerOpened extends Event with WorkerPoolMetric
  case object CircuitBreakerClosed extends Event with WorkerPoolMetric

  case class PoolSize(size: Int) extends Status with WorkerPoolMetric
  case class PoolUtilized(numWorkers: Int) extends Status with WorkerPoolMetric

}
