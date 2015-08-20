package com.iheart.poweramp.common.akka.patterns

import akka.actor.ActorRef
import scala.concurrent.duration._

package object queue {
  type QueueRef = ActorRef
  type QueueProcessorRef = ActorRef
  type WorkerRef = ActorRef
  type ResultChecker = PartialFunction[Any, Either[String, Any]]



  private[queue] case class Work(messageToDelegatee: Any, settings: WorkSetting = WorkSetting())

  private[queue] case class Rejected(work: Work, reason: String)

  case class WorkSetting(retry: Int = 0, timeout: FiniteDuration = 30.seconds, sendResultTo: Option[ActorRef] = None)

  case class CircuitBreakerSettings( closeDuration: FiniteDuration = 3.seconds,
                                     errorRateThreshold: Double = 1,
                                     historyLength: Int = 5 )

  case class BackPressureSettings( maxBufferSize: Int = 1000,
                                   thresholdForExpectedWaitTime: FiniteDuration = 1.minute,
                                   maxHistoryLength: FiniteDuration = 10.seconds)


  case class ProcessingWorkerPoolSettings( startingPoolSize: Int = 5,
                                           maxProcessingTime: Option[FiniteDuration] = None,
                                           minPoolSize: Int = 3)

}
