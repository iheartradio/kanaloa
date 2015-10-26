package com.iheart.workpipeline.metrics

import akka.actor._
import com.typesafe.config.{ Config, ConfigFactory }
import com.iheart.util.ConfigWrapper.ImplicitConfigWrapper

/**
 * Collector that sends metrics to StatsD
 *
 * @constructor create new collector with an existing StatsDClient
 * @param statsd StatsDClient
 * @param eventSampleRate sample rate for countable events (WorkEnqueued, WorkCompleted, etc)
 * @param statusSampleRate sample rate for gauged events (PoolSize, WorkQueueLength, etc)
 */
class StatsDMetricsCollector(
  statsd: StatsDClient,
  eventSampleRate: Double,
  statusSampleRate: Double)(implicit system: ActorSystem)
  extends MetricsCollector {

  /**
   * Auxilliary constructor that creates a StatsDClient from params
   *
   * @param prefix all StatsD metrics will be prefixed with this value
   * @param host StatsD host
   * @param port StatsD port, default 8125
   * @param eventSampleRate sample rate for countable events
   * @param statusSampleRate sample rate for gauged events
   */
  def this(
    prefix: String,
    host: String,
    port: Int = 8125,
    eventSampleRate: Double = 1.0,
    statusSampleRate: Double = 1.0)(implicit system: ActorSystem) =
    this(new StatsDClient(system, host, port, prefix = prefix), eventSampleRate, statusSampleRate)

  import Metric._

  private def gauge(key: String, value: Int, rate: Double = statusSampleRate) =
    statsd.gauge(key, value.toString, statusSampleRate)
  private def increment(key: String, rate: Double = eventSampleRate) =
    statsd.increment(key, 1, rate)

  /**
   * Always increment error counter regardless of sample rate.
   * If desired, this can be overridden with `override val`
   */
  val failureSampleRate: Double = 1.0

  def send(metric: Metric): Unit = metric match {
    case WorkEnqueued         ⇒ increment("queue.enqueued")
    case EnqueueRejected      ⇒ increment("queue.enqueueRejected")
    case WorkCompleted        ⇒ increment("work.completed")
    case WorkFailed           ⇒ increment("work.failed", failureSampleRate)
    case WorkTimedOut         ⇒ increment("work.timedOut")
    case CircuitBreakerOpened ⇒ increment("queue.circuitBreakerOpened")

    case PoolSize(size) ⇒
      gauge("pool.size", size)

    case PoolUtilized(numWorkers) ⇒
      gauge("pool.utilized", numWorkers)

    case DispatchWait(duration) ⇒
      statsd.timing("queue.waitTime", duration.toMillis.toInt, eventSampleRate)

    case WorkQueueLength(length) ⇒
      gauge("queue.length", length)

    case WorkQueueMaxLength(length) ⇒
      gauge("queue.maxLength", length)
  }
}

object StatsDMetricsCollector {
  /**
   * Create a StatsDMetricsCollector from typesafe Config object
   *
   * @param pipelineName used to determine the metrics prefix (`namespace.pipelineName`)
   * @param conf Config object with keys: namespace, host, port, eventSampleRate, statusSampleRate
   */
  def fromConfig(pipelineName: String, conf: Config)(implicit system: ActorSystem): StatsDMetricsCollector = {
    val namespace: String = conf.getOrElse[String]("namespace", "workPipeline")
    val prefix: String = List(namespace, pipelineName).filter(_.nonEmpty).mkString(".")

    new StatsDMetricsCollector(
      prefix = prefix,
      host = conf.getString("host"),
      port = conf.getOrElse[Int]("port", 8125),
      eventSampleRate = conf.getOrElse[Double]("eventSampleRate", 1.0),
      statusSampleRate = conf.getOrElse[Double]("statusSampleRate", 1.0))
  }
}

