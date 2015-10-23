package com.iheart.workpipeline.metrics

import akka.actor._
import com.typesafe.config.{Config, ConfigFactory}
import com.iheart.util.ConfigWrapper.ImplicitConfigWrapper

// TODO: add scaladoc
class StatsDMetricsCollector(
  statsd: StatsDClient,
  eventSampleRate: Double,
  statusSampleRate: Double)(implicit system: ActorSystem)
  extends MetricsCollector {

  def this(
    prefix: String,
    host: String,
    port: Int,
    eventSampleRate: Double = 1.0,
    statusSampleRate: Double = 1.0)(implicit system: ActorSystem) =
    this(new StatsDClient(system, host, port, prefix = prefix), eventSampleRate, statusSampleRate)

  import Metric._

  private def gauge(key: String, value: Int, rate: Double = statusSampleRate) =
    statsd.gauge(key, value.toString, statusSampleRate)
  private def increment(key: String, rate: Double = eventSampleRate) =
    statsd.increment(key, 1, rate)

  def send(metric: Metric): Unit = metric match {
    case WorkEnqueued => increment("queue.enqueued")
    case EnqueueRejected => increment("queue.enqueueRejected")
    case WorkCompleted => increment("work.completed")
    case WorkFailed => increment("work.failed", 1.0)
    case WorkTimedOut => increment("work.timedOut")

    case PoolSize(size) =>
      gauge("pool.size", size)

    case PoolUtilized(numWorkers) =>
      gauge("pool.utilized", numWorkers)

    case DispatchWait(duration) =>
      statsd.timing("queue.waitTime", duration.toMillis.toInt, eventSampleRate)

    case WorkQueueLength(length) =>
      gauge("queue.length", length)

    case WorkQueueMaxLength(length) =>
      gauge("queue.maxLength", length)
  }
}

object StatsDMetricsCollector {
  def fromConfig(pipelineName: String, conf: Config)(implicit system: ActorSystem): StatsDMetricsCollector = {
    val namespace: String = conf.getOrElse[String]("namespace", "workPipeline")
    val prefix: String = List(namespace, pipelineName).filter(_.nonEmpty).mkString(".")

    new StatsDMetricsCollector(
      prefix = prefix,
      host = conf.getString("host"),
      port = conf.getInt("port"),
      eventSampleRate = conf.getOrElse[Double]("eventSampleRate", 1.0),
      statusSampleRate = conf.getOrElse[Double]("statusSampleRate", 1.0)
    )
  }
}

