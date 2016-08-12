package kanaloa.reactive.dispatcher.metrics

import akka.actor._

/**
 * Collector that sends metrics to StatsD
 *
 * @constructor create new collector with an existing StatsDClient
 * @param statsd StatsDClient
 * @param eventSampleRate sample rate for countable events (WorkEnqueued, WorkCompleted, etc)
 * @param statusSampleRate sample rate for gauged events (PoolSize, WorkQueueLength, etc)
 */
class StatsDReporter(
  statsd:               StatsDClient,
  val eventSampleRate:  Double,
  val statusSampleRate: Double
)(implicit system: ActorSystem)
  extends Reporter {

  /**
   * Auxilliary constructor that creates a StatsDClient from params
   *
   * @param prefix all StatsD metrics will be prefixed with this value
   * @param settings
   */
  def this(
    prefix:   String,
    settings: StatsDMetricsReporterSettings
  )(implicit system: ActorSystem) =
    this(new StatsDClient(system, settings.host, settings.port, prefix = prefix), settings.eventSampleRate, settings.statusSampleRate)

  import Metric._

  private def gauge(key: String, value: Any, rate: Double = statusSampleRate) =
    statsd.gauge(key, value.toString, statusSampleRate)
  private def increment(key: String, rate: Double = eventSampleRate) =
    statsd.increment(key, 1, rate)

  /**
   * Always increment error counter regardless of sample rate.
   * If desired, this can be overridden with `override val`
   */
  val failureSampleRate: Double = 1.0

  def report(metric: Metric): Unit = metric match {
    case WorkReceived ⇒ increment("work.received")
    case WorkRejected ⇒ increment("work.rejected", Math.min(1d, eventSampleRate * 3d))

    case WorkCompleted(processTime) ⇒
      increment("work.completed")
      statsd.timing("work.processTime", processTime.toMillis.toInt, eventSampleRate)

    case WorkFailed           ⇒ increment("work.failed", failureSampleRate)
    case WorkTimedOut         ⇒ increment("work.timedOut", failureSampleRate)
    case CircuitBreakerOpened ⇒ increment("worker.circuitBreakerOpened", failureSampleRate)

    case PoolSize(size) ⇒
      gauge("pool.size", size)

    case DropRate(value) ⇒
      gauge("queue.dropRate", value)

    case PoolUtilized(numWorkers) ⇒
      gauge("pool.utilized", numWorkers)

    case BurstMode(inBurst) ⇒
      gauge("queue.burstMode", if (inBurst) 1 else 0)

    case WorkQueueExpectedWaitTime(duration) ⇒
      statsd.timing("queue.waitTime", duration.toMillis.toInt)

    case WorkQueueLength(length) ⇒
      gauge("queue.length", length)

  }
}

object StatsDReporter {
  /**
   * Create a StatsDMetricsCollector from typesafe Config object
   *
   * @param dispatcherName used to determine the metrics prefix (`namespace.dispatcherName`)
   * @param settings
   */
  def apply(dispatcherName: String, settings: StatsDMetricsReporterSettings)(implicit system: ActorSystem): StatsDReporter = {
    val prefix: String = List(settings.namespace, dispatcherName).filter(_.nonEmpty).mkString(".")

    new StatsDReporter(prefix, settings)
  }
}
