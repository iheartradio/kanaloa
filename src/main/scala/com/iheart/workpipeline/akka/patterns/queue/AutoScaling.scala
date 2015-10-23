package com.iheart.workpipeline.akka.patterns.queue

import akka.actor.{Props, Terminated, ActorLogging, Actor}
import com.iheart.workpipeline.akka.patterns.CommonProtocol.QueryStatus
import AutoScaling._
import QueueProcessor._
import java.time.{LocalDateTime, Duration ⇒ JDuration}
import com.iheart.workpipeline.akka.helpers.MessageScheduler
import com.iheart.workpipeline.akka.patterns.queue.Queue.QueueDispatchInfo
import com.iheart.workpipeline.akka.patterns.queue.QueueProcessor.ScaleTo
import com.iheart.workpipeline.akka.patterns.queue.Worker.{Working, WorkerStatus}
import com.iheart.workpipeline.metrics.{MetricsCollector, NoOpMetricsCollector, Metric}

import scala.concurrent.duration._
import scala.util.Random
import scala.language.implicitConversions

trait AutoScaling extends Actor with ActorLogging with MessageScheduler {
  val queue: QueueRef
  val processor: QueueProcessorRef
  val metricsCollector: MetricsCollector

  //accessible only for testing purpose
  private[queue] var perfLog: PerformanceLog = Map.empty
  private[queue] var underUtilizationStreak: Option[UnderUtilizationStreak] = None

  val settings: AutoScalingSettings

  import settings._
  final def receive: Receive = {
    delayedMsg(actionFrequency, OptimizeOrExplore)
    idle
  }

  private def watchingQueueAndProcessor: Receive = {
    case Terminated(`queue`) | Terminated(`processor`) | Queue.Retiring | QueueProcessor.ShuttingDown => context stop self
  }

  private def idle: Receive = watchingQueueAndProcessor orElse {
    case OptimizeOrExplore =>
      queue ! QueryStatus()
      delayedMsg(statusCollectionTimeout, StatusCollectionTimedOut)
      context become collectingStatus(SystemStatus())

    case StatusCollectionTimedOut => //nothing to worry about
  }

  private def collectingStatus(status: SystemStatus): Receive = watchingQueueAndProcessor orElse {
    case qdi: QueueDispatchInfo =>
      val waitDuration = qdi.avgDispatchDurationLowerBound
      if(waitDuration.isDefined) {
        processor ! QueryStatus()
        continueCollectingStatus(status.copy(dispatchWait = waitDuration))
      } else
        takeABreak()

    case RunningStatus(pool) =>
      pool.foreach(_ ! QueryStatus())
      continueCollectingStatus(status.copy(workerPool = Some(pool)))

    case StatusCollectionTimedOut =>
      log.error("Timedout to collect status from the queue and processor. Next time!")
      takeABreak()

    case Worker.Retiring =>
      continueCollectingStatus(status.copy(workerPool = status.workerPool.map(_ - sender)))

    case ws: WorkerStatus =>
      continueCollectingStatus(status.copy(workersStatus = status.workersStatus :+ ws))

    case msg => log.error(s"unexpected msg $msg")

  }

  private def takeABreak(): Unit = {
    context become idle
    delayedMsg(actionFrequency, OptimizeOrExplore)
  }


  private def continueCollectingStatus(newStatus: SystemStatus) = newStatus match {
    case SystemStatus(Some(qs), Some(wp), ws) if newStatus.collected =>
      val action = chooseAction(qs, wp, ws)
      log.info(s"Auto scaling $action is chosen. Current dispatch time: ${qs.toMillis}; Pool size: ${wp.size}")
      action.foreach(processor ! _)
      takeABreak()
    case _ =>
      context become collectingStatus(newStatus)
  }

  private def chooseAction(dispatchWait: Duration, workerPool: WorkerPool, workerStatus: List[WorkerStatus]): Option[ScaleTo] = {
    val utilization = workerStatus.count(_ == Working)

    val currentSize: PoolSize = workerPool.size
    val newEntry = PerformanceLogEntry(currentSize, dispatchWait, utilization, LocalDateTime.now)

    val fullyUtilized = utilization == currentSize

    // Send metrics
    metricsCollector.send(Metric.PoolSize(currentSize))
    metricsCollector.send(Metric.PoolUtilized(utilization))

    underUtilizationStreak = if (!fullyUtilized)
      underUtilizationStreak.map(s ⇒ s.copy(highestUtilization = Math.max(s.highestUtilization, utilization))) orElse Some(UnderUtilizationStreak(LocalDateTime.now, utilization))
    else None

    if (fullyUtilized) {
      val toUpdate = perfLog.get(currentSize).fold(dispatchWait) { oldSpeed ⇒
        val nanos = (oldSpeed.toNanos * (1d - weightOfLatestMetric)) + (dispatchWait.toNanos * weightOfLatestMetric)
        Duration.fromNanos(nanos)
      }
      perfLog =  perfLog + (currentSize → toUpdate)

      Some(
        if(newEntry.fullyUtilized && Random.nextDouble() < explorationRatio)
          explore(currentSize)
        else
          optimize(currentSize)
      )
    } else downsize

  }

  private def downsize: Option[ScaleTo] = {
    underUtilizationStreak.flatMap { streak ⇒
      if(streak.start.isBefore(LocalDateTime.now.minus(downsizeAfterUnderUtilization)))
        Some(ScaleTo((streak.highestUtilization * bufferRatio).toInt, Some("downsizing")))
      else
        None
    }
  }

  private def optimize(currentSize: PoolSize): ScaleTo = {


    val adjacentDispatchWaits: Map[PoolSize, Duration] = {
      def adjacency = (size: Int) => Math.abs(currentSize - size)
      val sizes = perfLog.keys.toSeq
      val numOfSizesEachSide = numOfAdjacentSizesToConsiderDuringOptimization / 2
      val leftBoundary=  sizes.filter(_ < currentSize).sortBy(adjacency).take(numOfSizesEachSide).lastOption.getOrElse(currentSize)
      val rightBoundary =  sizes.filter(_ >= currentSize).sortBy(adjacency).take(numOfSizesEachSide).lastOption.getOrElse(currentSize)
      perfLog.filter { case (size, _) => size >= leftBoundary && size <= rightBoundary }
    }

    val optimalSize = adjacentDispatchWaits.minBy(_._2)._1
    val scaleStep = Math.ceil((optimalSize - currentSize) / 2).toInt
    ScaleTo(Math.min(upperBound, currentSize + scaleStep), Some("optimizing"))
  }

  private def explore(currentSize: PoolSize): ScaleTo = {
    val change = Math.max(1, Random.nextInt(Math.ceil(currentSize * exploreStepSize).toInt))
    if(Random.nextDouble() < chanceOfScalingDownWhenFull)
      ScaleTo(currentSize - change, Some("exploring"))
    else
      ScaleTo(currentSize + change, Some("exploring"))
  }

  private implicit def durationToJDuration(d: FiniteDuration): JDuration = JDuration.ofNanos(d.toNanos)
}

object AutoScaling {
  case object OptimizeOrExplore
  case object StatusCollectionTimedOut

  private case class SystemStatus(dispatchWait: Option[Duration] = None,
                                  workerPool: Option[WorkerPool] = None,
                                  workersStatus: List[WorkerStatus] = Nil) {
    def collected: Boolean = (for {
      _ <- dispatchWait
      pool <- workerPool
    } yield workersStatus.length == pool.size).getOrElse(false)
  }
  type PoolSize = Int

  case class PerformanceLogEntry(poolSize: Int, dispatchWait: Duration, actualUtilization: Int, time: LocalDateTime) {
    def fullyUtilized = poolSize == actualUtilization
  }

  private[queue] case class UnderUtilizationStreak(start: LocalDateTime, highestUtilization: Int)

  private[queue] type PerformanceLog = Map[PoolSize, Duration]

  case class Default(queue: QueueRef,
                     processor: QueueProcessorRef,
                     settings: AutoScalingSettings,
                     metricsCollector: MetricsCollector = NoOpMetricsCollector) extends AutoScaling

  def default(queue: QueueRef,
              processor: QueueProcessorRef,
              settings: AutoScalingSettings,
              metricsCollector: MetricsCollector = NoOpMetricsCollector) =
    Props(Default(queue, processor, settings, metricsCollector))
}

