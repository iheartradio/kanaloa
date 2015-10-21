package com.iheart.workpipeline.metrics

import akka.actor._
import com.timgroup.statsd.{ StatsDClient, NonBlockingStatsDClient }
import scala.util.Random

class StatsDMetricCollector(statsd: StatsDClient, sampleRate: Double = 1.0)(implicit system: ActorSystem)
  extends MetricsCollector {

  lazy val actor: ActorRef = system.actorOf(Props(new StatsDActor(statsd)))

  def send(metric: Metric): Unit =
    if (sampleRate >= 1 || Random.nextDouble <= sampleRate) {
      actor ! metric
    }

  protected class StatsDActor(statsd: StatsDClient) extends Actor with ActorLogging {
    import Metric._

    private def increment(key: String) = statsd.count(key, 1, sampleRate)

    def receive: Receive = {
      case WorkEnqueued => increment("queue.enqueued")
      case EnqueueRejected => increment("queue.enqueueRejected")
      case WorkCompleted => increment("work.completed")
      case WorkFailed => increment("work.failed")
      case WorkTimedOut => increment("work.timedOut")

      case PoolSize(size) =>
        statsd.gauge("pool.size", size)

      case PoolUtilized(numWorkers) =>
        statsd.gauge("pool.utilized", numWorkers)

      case DispatchWait(duration) =>
        statsd.time("queue.waitTime", duration.toMillis)

      case WorkQueueLength(length) =>
        statsd.gauge("queue.length", length)

      case WorkQueueMaxLength(length) =>
        statsd.gauge("queue.maxLength", length)
    }
  }

}

object StatsDMetricCollector {
  def fromClient(statsd: StatsDClient, sampleRate: Double = 1.0)(implicit system: ActorSystem) =
    new StatsDMetricCollector(statsd, sampleRate)


  def apply(prefix: String,
            host: String,
            port: Int,
            sampleRate: Double = 1.0)(implicit system: ActorSystem) =
    new StatsDMetricCollector(new NonBlockingStatsDClient(prefix, host, port), sampleRate)
}

