package com.iheart.workpipeline.time

import java.time.LocalDateTime
import java.time.temporal.{ ChronoUnit, Temporal }

import scala.concurrent.duration._

object Java8TimeExtensions {

  implicit class WithDuration(self: LocalDateTime) {
    def until(that: Temporal): FiniteDuration = {
      val nanos = self.until(that, ChronoUnit.NANOS)
      FiniteDuration(nanos, NANOSECONDS)
    }
  }

}
