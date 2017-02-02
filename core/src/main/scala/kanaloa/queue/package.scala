import akka.actor.ActorRef
import scala.concurrent.duration._
import java.time.LocalDateTime
import kanaloa.util.Java8TimeExtensions._
package kanaloa {

  package object queue {
    type QueueRef = ActorRef
    type WorkerPoolManagerRef = ActorRef
    type WorkerRef = ActorRef
  }

}

package kanaloa.queue {

  private[queue] case class Work[T](
    messageToDelegatee: T,
    replyTo:            Option[ActorRef] = None,
    settings:           WorkSettings     = WorkSettings(),
    receivedAt:         LocalDateTime    = LocalDateTime.now //a < 1 microsecond cost
  ) {
    def expired: Boolean =
      settings.requestTimeout.fold(false) { to ⇒ receivedAt.until(LocalDateTime.now) > to }
  }

  private[queue] case class Rejected[T](work: Work[T], reason: String)

  case class WorkSettings(
    serviceTimeout:            FiniteDuration         = 30.seconds,
    lengthOfDisplayForMessage: Int                    = 300,
    atLeastOnce:               Boolean                = false,
    requestTimeout:            Option[FiniteDuration] = None

  ) {
    requestTimeout.foreach { rt ⇒
      assert(rt > serviceTimeout, "request timeout, if set, must be longer than service timeout")
    }
  }

  /**
   * see reference.conf
   */
  case class CircuitBreakerSettings(
    openDurationBase:      FiniteDuration = 3.seconds,
    timeoutCountThreshold: Int            = 3
  )

  /**
   * see reference.conf
   */
  case class WorkerPoolSettings(
    startingPoolSize:        Int            = 5,
    minPoolSize:             Int            = 3,
    maxPoolSize:             Int            = 400,
    replenishSpeed:          FiniteDuration = 1.second,
    logRouteeRetrievalError: Boolean        = true,
    defaultShutdownTimeout:  FiniteDuration = 30.seconds
  )

  /**
   * see reference.conf
   */
  case class AutothrottleSettings(
    chanceOfScalingDownWhenFull: Double         = 0.3,
    resizeInterval:              FiniteDuration = 5.seconds,
    optimizationMinRange:        Int            = 6,
    optimizationRangeRatio:      Double         = 0.3,
    maxExploreStepSize:          Int            = 4,
    explorationRatio:            Double         = 0.4,
    weightOfLatestMetric:        Double         = 0.5,
    weightOfLatency:             Double         = 0.2
  )

}
