package kanaloa.reactive.dispatcher.metrics

import akka.actor.ActorSystem
import com.typesafe.config.Config

trait MetricsCollector {
  def send(metric: Metric): Unit
}

object NoOpMetricsCollector extends MetricsCollector {
  def send(metric: Metric): Unit = () // Do nothing
}

object MetricsCollector {
  /** If statsd config exists, create StatsD, otherwise NoOp */
  def fromConfig(dispatcherName: String, config: Config)(implicit system: ActorSystem): MetricsCollector = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    config.as[Option[StatsDMetricsCollectorSettings]]("metrics.statsd").fold[MetricsCollector](NoOpMetricsCollector) { settings ⇒
      StatsDMetricsCollector.apply(dispatcherName, settings)
    }
  }
}

