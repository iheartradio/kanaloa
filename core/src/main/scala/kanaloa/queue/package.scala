import akka.actor.ActorRef
import scala.concurrent.duration._

package kanaloa {

  package object queue {
    type QueueRef = ActorRef
    type QueueProcessorRef = ActorRef
    type WorkerRef = ActorRef
  }

}

package kanaloa.queue {

  private[queue] case class Work(messageToDelegatee: Any, replyTo: Option[ActorRef] = None, settings: WorkSettings = WorkSettings())

  private[queue] case class Rejected(work: Work, reason: String)

  case class WorkSettings(
    retry:                     Int            = 0,
    timeout:                   FiniteDuration = 30.seconds,
    lengthOfDisplayForMessage: Int            = 300
  )

  /**
   * see reference.conf
   */
  case class CircuitBreakerSettings(
    openDurationBase:      FiniteDuration = 3.seconds,
    timeoutCountThreshold: Double         = 3
  )

  /**
   * see reference.conf
   */
  case class ProcessingWorkerPoolSettings(
    startingPoolSize:         Int            = 5,
    minPoolSize:              Int            = 3,
    maxPoolSize:              Int            = 400,
    healthCheckInterval:      FiniteDuration = 1.seconds,
    logRouteeRetrievalError:  Boolean        = true,
    shutdownOnAllWorkerDeath: Boolean        = true,
    defaultShutdownTimeout:   FiniteDuration = 30.seconds
  )

  /**
   * see reference.conf
   */
  case class AutothrottleSettings(
    chanceOfScalingDownWhenFull:   Double         = 0.3,
    resizeInterval:                FiniteDuration = 5.seconds,
    downsizeAfterUnderUtilization: FiniteDuration = 72.hours,
    optimizationMinRange:          Int            = 6,
    optimizationRangeRatio:        Double         = 0.3,
    maxExploreStepSize:            Int            = 4,
    downsizeRatio:                 Double         = 0.8,
    explorationRatio:              Double         = 0.4,
    weightOfLatestMetric:          Double         = 0.5,
    weightOfLatency:               Double         = 0.2
  )

}
