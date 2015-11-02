package kanaloa.reactive.dispatcher.metrics

import akka.actor._
import com.typesafe.config.Config
import kanaloa.util.ConfigWrapper
import kanaloa.util.ConfigWrapper.ImplicitConfigWrapper

/**
 * Collector that sends metrics to StatsD
 *
 * @constructor create new collector with an existing StatsDClient
 * @param statsd StatsDClient
 * @param eventSampleRate sample rate for countable events (WorkEnqueued, WorkCompleted, etc)
 * @param statusSampleRate sample rate for gauged events (PoolSize, WorkQueueLength, etc)
 */
class StatsDMetricsCollector(
  statsd:               StatsDClient,
  val eventSampleRate:  Double,
  val statusSampleRate: Double
)(implicit system: ActorSystem)
  extends MetricsCollector {

  /**
   * Auxilliary constructor that creates a StatsDClient from params
   *
   * @param prefix all StatsD metrics will be prefixed with this value
   * @param settings
   */
  def this(
    prefix:   String,
    settings: StatsDMetricsCollectorSettings
  )(implicit system: ActorSystem) =
    this(new StatsDClient(system, settings.host, settings.port, prefix = prefix), settings.eventSampleRate, settings.statusSampleRate)

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
      statsd.timing("queue.avgProcessTime", duration.toMillis.toInt, eventSampleRate)

    case WorkQueueExpectedWaitTime(duration) ⇒
      statsd.timing("queue.waitTime", duration.toMillis.toInt, eventSampleRate)

    case WorkQueueLength(length) ⇒
      gauge("queue.length", length)

  }
}

object StatsDMetricsCollector {
  /**
   * Create a StatsDMetricsCollector from typesafe Config object
   *
   * @param dispatcherName used to determine the metrics prefix (`namespace.dispatcherName`)
   * @param settings
   */
  def apply(dispatcherName: String, settings: StatsDMetricsCollectorSettings)(implicit system: ActorSystem): StatsDMetricsCollector = {
    val prefix: String = List(settings.namespace, dispatcherName).filter(_.nonEmpty).mkString(".")

    new StatsDMetricsCollector(prefix, settings)
  }
}

