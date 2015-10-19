package com.iheart.workpipeline.metrics

import akka.actor._
import com.timgroup.statsd.{ StatsDClient, NonBlockingStatsDClient }
import scala.util.Random

class StatsDMetricCollector(system: ActorSystem, statsd: StatsDClient, sampleRate: Double = 1.0)
  extends MetricsCollector {

  lazy val actor: ActorRef = system.actorOf(Props(new StatsDActor(statsd)))

  def send(metric: Metric): Unit =
    if (sampleRate >= 1 || Random.nextDouble <= sampleRate) {
      actor ! metric
    }

  protected class StatsDActor(statsd: StatsDClient) extends Actor with ActorLogging {
    private def increment(key: String) = statsd.count(key, 1, sampleRate)

    def receive: Receive = {
      case Metric.WorkEnqueued => increment("queue.enqueued")
      case Metric.WorkDequeued => increment("queue.dequeued")
      case Metric.EnqueueRejected => increment("queue.enqueueRejected")
      case Metric.WorkCompleted => increment("work.completed")
      case Metric.WorkFailed => increment("work.failed")
      case Metric.WorkTimedOut => increment("work.timedOut")

      case Metric.PoolSize(size, utilized) =>
        statsd.gauge("pool.size", size)
        statsd.gauge("pool.utilized", utilized)

      case Metric.AverageWaitTime(duration) =>
        statsd.time("queue.waitTime", duration.toMillis)

      case Metric.WorkQueueLength(length) =>
        statsd.gauge("queue.length", length)

      case Metric.WorkQueueMaxLength(length) =>
        statsd.gauge("queue.maxLength", length)

      case _ => // Ignore
    }
  }

}

