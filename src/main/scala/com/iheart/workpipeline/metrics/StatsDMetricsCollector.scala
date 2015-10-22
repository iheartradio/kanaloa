package com.iheart.workpipeline.metrics

import akka.actor._
import com.typesafe.config.{Config, ConfigFactory}

class StatsDMetricsCollector(statsd: StatsDClient)(implicit system: ActorSystem)
  extends MetricsCollector {

  def this(prefix: String, host: String, port: Int, sampleRate: Double = 1.0)(implicit system: ActorSystem) =
    this(new StatsDClient(system, host, port, prefix = prefix, defaultSampleRate = sampleRate))

  import Metric._

  private def gauge(key: String, value: Int) = statsd.gauge(key, value.toString)

  def send(metric: Metric): Unit = metric match {
    case WorkEnqueued => statsd.increment("queue.enqueued")
    case EnqueueRejected => statsd.increment("queue.enqueueRejected")
    case WorkCompleted => statsd.increment("work.completed")
    case WorkFailed => statsd.increment("work.failed")
    case WorkTimedOut => statsd.increment("work.timedOut")

    case PoolSize(size) =>
      gauge("pool.size", size)

    case PoolUtilized(numWorkers) =>
      gauge("pool.utilized", numWorkers)

    case DispatchWait(duration) =>
      statsd.timing("queue.waitTime", duration.toMillis.toInt)

    case WorkQueueLength(length) =>
      gauge("queue.length", length)

    case WorkQueueMaxLength(length) =>
      gauge("queue.maxLength", length)
  }
}

object StatsDMetricsCollector {
  def fromConfig(conf: Config)(implicit system: ActorSystem): StatsDMetricsCollector =
    new StatsDMetricsCollector(
      prefix = conf.getString("prefix"),
      host = conf.getString("host"),
      port = conf.getInt("port"),
      sampleRate = conf.getDouble("sampleRate")
    )
}

