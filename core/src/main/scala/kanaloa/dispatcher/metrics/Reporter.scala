package kanaloa.dispatcher.metrics

import akka.actor.ActorSystem
import com.typesafe.config.{ConfigFactory, Config}

trait Reporter {
  def report(metric: Metric): Unit
}

object Reporter {
  case object StatsDClientNotProvidedException extends Exception

  /** If statsD config exists, create StatsDReporter, otherwise None */
  private[dispatcher] def fromConfig(
    dispatcherName: String,
    config:         Config,
    statsDClient:   Option[StatsDClient]
  ): Option[Reporter] = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._

    val defaultSettings = config.as[Option[StatsDMetricsReporterSettings]]("metrics.statsD").filter(_ ⇒ config.getOrElse("metrics.enabled", true))
    defaultSettings.map(StatsDReporter(dispatcherName, _, statsDClient.getOrElse(throw StatsDClientNotProvidedException)))
  }
}
