package kanaloa.dispatcher.metrics

sealed trait MetricsReporterSettings

case class StatsDMetricsReporterSettings(
  host:             String,
  namespace:        String = "kanaloa-dispatchers",
  port:             Int    = 8125,
  eventSampleRate:  Double = 0.25,
  statusSampleRate: Double = 1
) extends MetricsReporterSettings
