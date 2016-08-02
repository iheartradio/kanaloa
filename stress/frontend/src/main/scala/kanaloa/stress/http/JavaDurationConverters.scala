package kanaloa.stress.http

import java.time.{ Duration => JDuration }

import scala.concurrent.duration.Duration

private[kanaloa] object JavaDurationConverters {
  final implicit class JavaDurationOps(val self: JDuration) extends AnyVal {
    def asScala: Duration = Duration.fromNanos(self.toNanos)
  }

  final implicit class ScalaDurationOps(val self: Duration) extends AnyVal {
    def asJava: JDuration = JDuration.ofNanos(self.toNanos)
  }
}
