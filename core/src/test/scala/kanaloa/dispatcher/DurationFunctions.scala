package kanaloa.dispatcher

import java.time.LocalDateTime

import scala.concurrent.duration.Duration

object DurationFunctions {
  implicit class DurationExtensions(duration: Duration) {
    def ago: LocalDateTime = LocalDateTime.now.minusNanos(duration.toNanos)
  }
}
