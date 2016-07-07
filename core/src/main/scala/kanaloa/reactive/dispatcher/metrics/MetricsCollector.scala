package kanaloa.reactive.dispatcher.metrics

class MetricsCollector(val reporter: Option[Reporter]) {

  def send(metrics: Metric): Unit = {
    reporter.foreach(_.report(metrics))
  }
}
