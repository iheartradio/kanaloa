
import akka.actor.ActorRef

import scala.concurrent.duration._

package kanaloa.reactive.dispatcher {

  package object queue {
    type QueueRef = ActorRef
    type QueueProcessorRef = ActorRef
    type WorkerRef = ActorRef
  }

}

package kanaloa.reactive.dispatcher.queue {

  private[queue] case class Work(messageToDelegatee: Any, replyTo: Option[ActorRef] = None, settings: WorkSettings = WorkSettings())

  private[queue] case class Rejected(work: Work, reason: String)

  case class WorkSettings(retry: Int = 0, timeout: FiniteDuration = 30.seconds)

  case class CircuitBreakerSettings(
    closeDuration:      FiniteDuration = 3.seconds,
    errorRateThreshold: Double         = 1,
    historyLength:      Int            = 10
  )

  /**
   *
   * @param startingPoolSize
   * @param maxProcessingTime timeout for the whole queue to be processed
   * @param minPoolSize
   */
  case class ProcessingWorkerPoolSettings(
    startingPoolSize:  Int                    = 5,
    maxProcessingTime: Option[FiniteDuration] = None,
    minPoolSize:       Int                    = 3,
    maxPoolSize:       Int                    = 400
  )

  /**
   *
   * @param chanceOfScalingDownWhenFull chance of scaling down when the worker pool is fully utlized
   * @param scalingInterval  duration between each scaling attempt
   * @param downsizeAfterUnderUtilization start to downsize after underutilized for period, should be long enough to include at least one traffic cycle.
   * @param numOfAdjacentSizesToConsiderDuringOptimization during optimization, it only looks at this number of adjacent pool sizes (adjacent to current pool size), to figure out the optimal pool size to move to
   * @param exploreStepSize during exploration, it takes as big a step as this size. It's a ratio to the current pool size, so if the current size is 10 and the exploreStepSize is 0.2, the exploration will be within a range between 8 and 12
   * @param downsizeRatio during downsizing, it will downsize to the largest number of concurrently occupied workers it has seen plus a buffer zone, this downsizeRatio determines the buffer size.
   * @param explorationRatio chance of doing a exploration vs an optimization
   * @param statusCollectionTimeout maximum time allowed when autoscaler collects status from queue, queueProcessor and all workers
   */
  case class AutoScalingSettings(
    chanceOfScalingDownWhenFull:                    Double         = 0.1,
    scalingInterval:                                FiniteDuration = 5.seconds,
    downsizeAfterUnderUtilization:                  FiniteDuration = 72.hours,
    numOfAdjacentSizesToConsiderDuringOptimization: Int            = 12,
    exploreStepSize:                                Double         = 0.1,
    downsizeRatio:                                  Double         = 0.8,
    explorationRatio:                               Double         = 0.4,
    weightOfLatestMetric:                           Double         = 0.5,
    statusCollectionTimeout:                        FiniteDuration = 30.seconds
  )

}
