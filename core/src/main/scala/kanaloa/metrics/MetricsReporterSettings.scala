package kanaloa.metrics

sealed trait MetricsReporterSettings

case class StatsDMetricsReporterSettings(
  namespace:        String = "kanaloa-dispatchers",
  eventSampleRate:  Double = 0.25,
  statusSampleRate: Double = 1
) extends MetricsReporterSettings
