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

    import Metric._

    def receive: Receive = {
      case WorkEnqueued => increment("queue.enqueued")
      case WorkDequeued => increment("queue.dequeued")
      case EnqueueRejected => increment("queue.enqueueRejected")
      case WorkCompleted => increment("work.completed")
      case WorkFailed => increment("work.failed")
      case WorkTimedOut => increment("work.timedOut")

      case PoolSize(size) =>
        statsd.gauge("pool.size", size)

      case PoolUtilized(numWorkers) =>
        statsd.gauge("pool.utilized", numWorkers)

      case AverageWaitTime(duration) =>
        statsd.time("queue.waitTime", duration.toMillis)

      case WorkQueueLength(length) =>
        statsd.gauge("queue.length", length)

      case WorkQueueMaxLength(length) =>
        statsd.gauge("queue.maxLength", length)
    }
  }

}

