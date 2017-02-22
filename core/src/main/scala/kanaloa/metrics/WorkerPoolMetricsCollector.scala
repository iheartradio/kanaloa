package kanaloa.metrics

import akka.actor._
import kanaloa.queue.WorkerPoolSampler
import kanaloa.metrics.Metric.{WorkerPoolMetric}

/**
 *  A metrics collector to which all [[kanaloa.metrics.Metric.WorkerPoolMetric]] are sent to.
 *  This can be mixed in to inject other metrics related behavior, see `kanaloa.queue.WorkerPoolSampler`
 */
trait WorkerPoolMetricsCollector extends Actor {

  def reporter: Option[Reporter]
  def metricsForwardTo: Option[ActorRef]

  protected def handle(metric: WorkerPoolMetric)(pf: PartialFunction[WorkerPoolMetric, Unit]): Unit = {
    report(metric)
    pf.applyOrElse(metric, (_: WorkerPoolMetric) ⇒ ())
  }

  protected final def report(metric: WorkerPoolMetric): Unit = {
    reporter.foreach(_.report(metric))
    metricsForwardTo.foreach(_ ! metric)
  }

}

