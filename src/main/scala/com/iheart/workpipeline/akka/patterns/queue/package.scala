package com.iheart.workpipeline.akka.patterns

import akka.actor.ActorRef
import scala.concurrent.duration._

package object queue {
  type QueueRef = ActorRef
  type QueueProcessorRef = ActorRef
  type WorkerRef = ActorRef
  type ResultChecker = PartialFunction[Any, Either[String, Any]]
}

package queue {
  private[queue] case class Work(messageToDelegatee: Any, settings: WorkSettings = WorkSettings())

  private[queue] case class Rejected(work: Work, reason: String)

  case class WorkSettings(retry: Int = 0, timeout: FiniteDuration = 30.seconds, sendResultTo: Option[ActorRef] = None)

  case class CircuitBreakerSettings(
    closeDuration: FiniteDuration = 3.seconds,
    errorRateThreshold: Double = 1,
    historyLength: Int = 10
  )

  case class BackPressureSettings(
    maxBufferSize: Int = 1000,
    thresholdForExpectedWaitTime: FiniteDuration = 1.minute,
    maxHistoryLength: FiniteDuration = 10.seconds
  )

  /**
   *
   * @param startingPoolSize
   * @param maxProcessingTime timeout for the whole queue to be processed
   * @param minPoolSize
   */
  case class ProcessingWorkerPoolSettings(
    startingPoolSize: Int = 5,
    maxProcessingTime: Option[FiniteDuration] = None,
    minPoolSize: Int = 3
  )

  /**
   *
   * @param chanceOfScalingDownWhenFull chance of scaling down when the worker pool is fully utlized
   * @param actionFrequency  duration between each scaling attempt
   * @param downsizeAfterUnderUtilization start to downsize after underutilized for period, should be long enough to include at least one traffic cycle.
   * @param numOfAdjacentSizesToConsiderDuringOptimization during optimization, it only look at this number of adjacent pool sizes (adjacent to current pool size), to figure out the optimal pool size to move to
   * @param exploreStepSize during exploration, it take as big a step as this size. It's a ratio to the current pool size, so if the current size is 10 and the exploreStepSize is 0.2, the exploration will be within a range between 8 and 12
   * @param bufferRatio during downsizing, it will downsize to the largest number of concurrently occupied workers it has seen plus a buffer zone, this bufferRatio determines the buffer size.
   * @param explorationRatio chance of doing a exploration vs an optimization
   * @param statusCollectionTimeout maximum time allowed when autoscaler collects status from queue, queueProcessor and all workers
   */
  case class AutoScalingSettings(
    chanceOfScalingDownWhenFull: Double = 0.1,
    actionFrequency: FiniteDuration = 5.seconds,
    downsizeAfterUnderUtilization: FiniteDuration = 72.hours,
    numOfAdjacentSizesToConsiderDuringOptimization: Int = 12,
    exploreStepSize: Double = 0.1,
    bufferRatio: Double = 0.8,
    upperBound: Int = 300,
    explorationRatio: Double = 0.4,
    weightOfLatestMetric: Double = 0.5,
    statusCollectionTimeout: FiniteDuration = 30.seconds
  )

}
