package kanaloa.reactive.dispatcher.metrics

sealed trait MetricsReporterSettings

case class StatsDMetricsReporterSettings(
  host:             String,
  namespace:        String = "reactiveDispatchers",
  port:             Int    = 8125,
  eventSampleRate:  Double = 0.25,
  statusSampleRate: Double = 1
) extends MetricsReporterSettings
