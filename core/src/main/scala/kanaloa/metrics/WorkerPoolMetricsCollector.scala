package kanaloa.metrics

import akka.actor._
import kanaloa.queue.WorkerPoolSampler
import kanaloa.queue.Sampler.SamplerSettings
import kanaloa.metrics.Metric.{WorkerPoolMetric}

/**
 *  A metrics collector to which all [[WorkerPoolMetric]] are sent to.
 *  This can be mixed in to inject other metrics related behavior, see [[WorkerPoolSampler]]
 */
trait WorkerPoolMetricsCollector extends Actor {

  def reporter: Option[Reporter]

  protected def handle(metric: WorkerPoolMetric)(pf: PartialFunction[WorkerPoolMetric, Unit]): Unit = {
    report(metric)
    pf.applyOrElse(metric, (_: WorkerPoolMetric) ⇒ ())
  }

  protected def report(metric: WorkerPoolMetric): Unit =
    reporter.foreach(_.report(metric))

}

