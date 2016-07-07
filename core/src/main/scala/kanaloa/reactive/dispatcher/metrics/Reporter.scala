package kanaloa.reactive.dispatcher.metrics

import akka.actor.ActorSystem
import com.typesafe.config.{ConfigFactory, Config}

trait Reporter {
  def report(metric: Metric): Unit
}

object Reporter {
  /** If statsd config exists, create StatsDReporter, otherwise None */
  def fromConfig(dispatcherName: String, config: Config)(implicit system: ActorSystem): Option[Reporter] = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._

    val defaultSettings = config.as[Option[StatsDMetricsCollectorSettings]]("metrics.statsd").filter(_ ⇒ config.getOrElse("metrics.enabled", true))

    defaultSettings.map(StatsDReporter(dispatcherName, _))
  }
}
