package kanaloa.reactive.dispatcher.metrics

import akka.actor.ActorRefFactory

class MetricsCollector(val reporter: Option[Reporter])(implicit actorRefFactory: ActorRefFactory) {

  def send(metrics: Metric): Unit = {
    reporter.foreach(_.report(metrics))
  }
}

object MetricsCollector {
  def apply(reporter: Option[Reporter])(implicit actorRefFactory: ActorRefFactory): MetricsCollector = new MetricsCollector(reporter)
}
