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
      case Event.WorkEnqueued => increment("queue.enqueued")
      case Event.WorkDequeued => increment("queue.dequeued")
      case Event.EnqueueRejected => increment("queue.enqueueRejected")
      case Event.WorkCompleted => increment("work.completed")
      case Event.WorkFailed => increment("work.failed")
      case Event.WorkTimedOut => increment("work.timedOut")

      case Status.PoolSize(size, utilized) =>
        statsd.gauge("pool.size", size)
        statsd.gauge("pool.utilized", utilized)

      case Status.AverageWaitTime(duration) =>
        statsd.time("queue.waitTime", duration.toMillis)

      case Status.WorkQueueLength(length) =>
        statsd.gauge("queue.length", length)

      case Status.WorkQueueMaxLength(length) =>
        statsd.gauge("queue.maxLength", length)

      case _ => // Ignore
    }
  }

}

