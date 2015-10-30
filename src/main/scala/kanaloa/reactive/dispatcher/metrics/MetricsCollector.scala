package kanaloa.reactive.dispatcher.metrics

import akka.actor.ActorSystem
import com.typesafe.config.{ Config, ConfigFactory }
import kanaloa.util.ConfigWrapper
import kanaloa.util.ConfigWrapper.ImplicitConfigWrapper

trait MetricsCollector {
  def send(metric: Metric): Unit
}

object NoOpMetricsCollector extends MetricsCollector {
  def send(metric: Metric): Unit = () // Do nothing
}

object MetricsCollector {
  /** Create a MetricsCollector from a root typesafe Config */
  def fromRootConfig(dispatcherName: String, config: Config = ConfigFactory.load())(implicit system: ActorSystem) = {
    val path = "reactiveDispatcher.metrics"
    val metricsConfig: Config = config.getOrElse[Config](path, ConfigFactory.empty)
    fromConfig(dispatcherName, metricsConfig)
  }

  /** If statsd config exists, create StatsD, otherwise NoOp */
  def fromConfig(dispatcherName: String, config: Config)(implicit system: ActorSystem): MetricsCollector = {
    config.getOption[Config]("statsd").fold[MetricsCollector](NoOpMetricsCollector) { statsDConf ⇒
      StatsDMetricsCollector.fromConfig(dispatcherName, statsDConf)
    }
  }
}

