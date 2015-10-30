package kanaloa.reactive.dispatcher.metrics

sealed trait MetricsCollectorSettings

case class StatsDMetricsCollectorSettings(
  host:             String,
  namespace:        String = "ReactiveDispatchers",
  port:             Int    = 8125,
  eventSampleRate:  Double = 0.25,
  statusSampleRate: Double = 1
) extends MetricsCollectorSettings
