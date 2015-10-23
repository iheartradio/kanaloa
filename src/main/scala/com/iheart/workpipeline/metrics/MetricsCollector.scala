package com.iheart.workpipeline.metrics

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}
import com.iheart.util.ConfigWrapper.ImplicitConfigWrapper

trait MetricsCollector {
  def send(metric: Metric): Unit
}

object NoOpMetricsCollector extends MetricsCollector {
  def send(metric: Metric): Unit = () // Do nothing
}

object MetricsCollector {
  /** Create a MetricsCollector from a root typesafe Config */
  def fromRootConfig(pipelineName: String, config: Config = ConfigFactory.load())(implicit system: ActorSystem) = {
    val path = "workPipeline.metrics"
    val metricsConfig: Config = config.getOrElse[Config](path, ConfigFactory.empty)
    fromConfig(pipelineName, metricsConfig)
  }

  /** If statsD config exists, create StatsD, otherwise NoOp */
  def fromConfig(pipelineName: String, config: Config)(implicit system: ActorSystem): MetricsCollector = {
    config.getOption[Config]("statsD").fold[MetricsCollector](NoOpMetricsCollector) { statsDConf =>
      StatsDMetricsCollector.fromConfig(pipelineName, statsDConf)
    }
  }
}

