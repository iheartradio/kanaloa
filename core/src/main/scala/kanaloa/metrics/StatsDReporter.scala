package kanaloa.metrics

import akka.actor._

/**
 * Collector that sends metrics to StatsD
 *
 * @constructor create new collector with an existing StatsDClient
 * @param statsD StatsDClient
 * @param eventSampleRate sample rate for countable events (WorkEnqueued, WorkCompleted, etc)
 * @param statusSampleRate sample rate for gauged events (PoolSize, WorkQueueLength, etc)
 */
class StatsDReporter(
  statsD:               StatsDClient,
  prefix:               String,
  val eventSampleRate:  Double,
  val statusSampleRate: Double
)
  extends Reporter {

  import Metric._
  private def fullKey(key: String) = prefix + "." + key

  private def gauge(key: String, value: Any, rate: Double = statusSampleRate) =
    statsD.gauge(fullKey(key), value.toString, statusSampleRate)

  private def increment(key: String, rate: Double = eventSampleRate) =
    statsD.increment(fullKey(key), 1, rate)

  private def timing(key: String, millis: Int, rate: Double = eventSampleRate) =
    statsD.timing(fullKey(key), millis, rate)

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
      timing("work.processTime", processTime.toMillis.toInt, eventSampleRate)

    case WorkFailed           ⇒ increment("work.failed", failureSampleRate)
    case WorkTimedOut         ⇒ increment("work.timedOut", failureSampleRate)
    case CircuitBreakerOpened ⇒ increment("worker.circuitBreakerOpened", failureSampleRate)

    case PoolSize(size) ⇒
      gauge("pool.size", size)

    case DropRate(value) ⇒
      gauge("queue.dropRate", value)

    case PoolUtilized(numWorkers) ⇒ //todo: move this to the worker pool sampler side.
      gauge("pool.utilized", numWorkers)

    case BurstMode(inBurst) ⇒
      gauge("queue.burstMode", if (inBurst) 1 else 0)

    case WorkQueueExpectedWaitTime(duration) ⇒
      timing("queue.waitTime", duration.toMillis.toInt)

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
  def apply(dispatcherName: String, settings: StatsDMetricsReporterSettings, statsDClient: StatsDClient): StatsDReporter = {

    import java.net._
    val localhost = InetAddress.getLocalHost.getHostAddress.replace(".", "_")

    val prefix: String = List(settings.namespace, dispatcherName, localhost).filter(_.nonEmpty).mkString(".")

    new StatsDReporter(statsDClient, prefix, settings.eventSampleRate, settings.statusSampleRate)
  }
}
