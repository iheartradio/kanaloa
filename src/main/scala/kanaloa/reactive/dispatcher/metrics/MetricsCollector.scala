package kanaloa.reactive.dispatcher.metrics

import akka.actor.ActorSystem
import com.typesafe.config.{ConfigFactory, Config}

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

    val defaultSettings = config.as[Option[StatsDMetricsCollectorSettings]]("metrics.statsd").filter(_ ⇒ config.getOrElse("metrics.enabled", true))

    defaultSettings.fold(NoOpMetricsCollector: MetricsCollector)(StatsDMetricsCollector(dispatcherName, _))
  }
}
