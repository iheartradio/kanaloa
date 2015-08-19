package com.iheart.poweramp.common.akka.patterns.queue

import java.time.LocalDateTime

import akka.actor.{Props, Terminated, ActorLogging, Actor}
import com.iheart.poweramp.common.akka.helpers.MessageScheduler
import com.iheart.poweramp.common.akka.patterns.CommonProtocol.QueryStatus
import com.iheart.poweramp.common.akka.patterns.queue.AutoScaling._
import com.iheart.poweramp.common.akka.patterns.queue.Queue.QueueDispatchInfo
import com.iheart.poweramp.common.akka.patterns.queue.QueueProcessor._
import com.iheart.poweramp.common.akka.patterns.queue.Worker.{Working, WorkerStatus}
import com.iheart.poweramp.common.collection.FiniteCollection.FiniteQueue

import scala.concurrent.duration._
import scala.util.Random

/***
  * Periodically try to improve the pool size by changing it randomly by +1/-1 unit.
  * If the new size improves performance then set this as the new pool size.
  * Be careful to only measure pool size performance when the pool is maxed out.
  * There’s no point measuring pool size performance when the pool is only partly used,
  * because performance isn’t being constrained by the size of the pool unless the pool is fully used.
  * Periodically shrink the pool size if it hasn’t been maxed out in some time period.
  * If the pool is never maxed out then it’s probably too big.
  * Set the time period to a long enough value that it spans the typical gap between peak traffic periods, e.g. 1 day.
  */
trait AutoScaling extends Actor with ActorLogging with MessageScheduler {
  val queue: QueueRef
  val processor: QueueProcessorRef

  //accessible only for testing purpose
  private[queue] var perfLog: PerformanceLog = Vector.empty

  def chanceOfScalingDownWhenFull = 0.1

  def actionFrequency: FiniteDuration = 15.seconds
  
  def relevantPeriodInHours = 72

  def numOfAdjacentSizesToConsiderDuringOptimization = 6

  def exploreStepSize = 0.1

  def bufferRatio = 0.1

  def logLength = 10000

  def explorationRatio = 0.4

  def statusCollectionTimeout = 30.seconds

  def receive: Receive = {
    delayedMsg(actionFrequency, OptimizeOrExplore)
    idle
  }

  def watchingQueueAndProcessor: Receive = {
    case Terminated(`queue`) | Terminated(`processor`) | Queue.Retiring | QueueProcessor.ShuttingDown => context stop self
  }

  def idle: Receive = watchingQueueAndProcessor orElse {
    case OptimizeOrExplore =>
      queue ! QueryStatus()
      delayedMsg(statusCollectionTimeout, StatusCollectionTimedOut)
      context become collectingStatus(SystemStatus())

    case StatusCollectionTimedOut => //nothing to worry about
  }

  def collectingStatus(status: SystemStatus): Receive = watchingQueueAndProcessor orElse {
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
    val newEntry = PerformanceLogEntry(workerPool.size, dispatchWait, utilization, LocalDateTime.now)
    perfLog = perfLog.enqueueFinite(newEntry, logLength)

    val (maxedLogs, unUtilizedLogs) = recentLogsWithin(relevantPeriodInHours).partition(_.fullyUtilized)

    if (maxedLogs.isEmpty) {

      val enoughUnUtilizedHistory = unUtilizedLogs.headOption.map(_.time.isBefore(LocalDateTime.now.minusHours(relevantPeriodInHours - 1))).getOrElse(false)
      if (enoughUnUtilizedHistory) {
        val maxUtilization = unUtilizedLogs.maxBy(_.actualUtilization).actualUtilization
        val downsizeTo = maxUtilization * (1 + bufferRatio)
        Some(ScaleTo(downsizeTo.toInt, Some("downsizing")))
      }
      else
        None
    } else
      Some(
        if(newEntry.fullyUtilized && Random.nextDouble() < explorationRatio)
          explore(workerPool.size)
        else
          optimize(workerPool.size, maxedLogs)
      )

  }

  private def recentLogsWithin(hours: Int) = perfLog.takeRightWhile(_.time.isAfter(LocalDateTime.now.minusHours(hours)))


  private def optimize(currentSize: PoolSize, relevantLogs: PerformanceLog): ScaleTo = {

    val avgDispatchWaitForEachSize: Map[PoolSize, Duration] = relevantLogs.groupBy(_.poolSize).mapValues{ logs =>
      if(logs.length > 1) {
        val init = logs.init
        val avgOfInit = init.foldLeft[Duration](0.millisecond)(_ + _.dispatchWait) / init.size
        (avgOfInit + logs.last.dispatchWait) / 2 //half weight on the latest speed, todo: this math could be improved.
      } else logs.head.dispatchWait
    }

    val adjacentDispatchDurations: Map[PoolSize, Duration] = {
      def adjacency = (size: Int) => Math.abs(currentSize - size)
      val sizes = avgDispatchWaitForEachSize.keys.toSeq
      val numOfSizesEachSide = numOfAdjacentSizesToConsiderDuringOptimization / 2
      val leftBoundary=  sizes.filter(_ < currentSize).sortBy(adjacency).take(numOfSizesEachSide).lastOption.getOrElse(currentSize)
      val rightBoundary =  sizes.filter(_ >= currentSize).sortBy(adjacency).take(numOfSizesEachSide).lastOption.getOrElse(currentSize)
      avgDispatchWaitForEachSize.filter { case (size, _) => size >= leftBoundary && size <= rightBoundary }
    }

    val optimalSize = adjacentDispatchDurations.minBy(_._2)._1
    val scaleStep = Math.ceil((optimalSize - currentSize) / 2).toInt
    ScaleTo(currentSize + scaleStep, Some("optimizing"))
  }

  private def explore(currentSize: PoolSize): ScaleTo = {
    val change = Math.max(1, Random.nextInt(Math.ceil(currentSize * exploreStepSize).toInt))
    if(Random.nextDouble() < chanceOfScalingDownWhenFull)
      ScaleTo(currentSize - change, Some("exploring"))
    else
      ScaleTo(currentSize + change, Some("exploring"))
  }


}

object AutoScaling {
  case object OptimizeOrExplore
  case object StatusCollectionTimedOut

  private case class SystemStatus(dispatchWait: Option[Duration] = None,
                                  workerPool: Option[WorkerPool] = None,
                                  workersStatus: List[WorkerStatus] = Nil ) {
    def collected: Boolean = (for {
      _ <- dispatchWait
      pool <- workerPool
    } yield workersStatus.length == pool.size).getOrElse(false)
  }
  type PoolSize = Int

  case class PerformanceLogEntry(poolSize: Int, dispatchWait: Duration, actualUtilization: Int, time: LocalDateTime) {
    def fullyUtilized = poolSize == actualUtilization
  }


  type PerformanceLog = Vector[PerformanceLogEntry]

  case class Default(queue: QueueRef, processor: QueueProcessorRef) extends AutoScaling

  def default(queue: QueueRef, processor: QueueProcessorRef) = Props(Default(queue, processor))
}
