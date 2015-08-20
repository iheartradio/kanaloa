package com.iheart.poweramp.common.akka.patterns.queue

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import akka.actor._
import com.iheart.poweramp.common.akka.helpers.MessageScheduler
import com.iheart.poweramp.common.akka.patterns.CommonProtocol.QueryStatus
import com.iheart.poweramp.common.akka.patterns.queue.Queue.EnqueueRejected.{OverCapacity, Reason}
import com.iheart.poweramp.common.akka.patterns.queue.Queue._
import scala.annotation.tailrec
import scala.collection.immutable.{ Queue => ScalaQueue }
import scala.concurrent.duration._
import com.iheart.poweramp.common.collection.FiniteCollection._
import Queue.QueueStatus
import com.iheart.poweramp.common.time.Java8TimeExtensions._


trait Queue extends Actor with ActorLogging with MessageScheduler {

  def defaultWorkSettings: WorkSetting
  protected def bufferHistoryLength: Int
  protected val historySampleRateInMills: Int = 500

  type ReceiveEnqueue = PartialFunction[Enqueue, Any]

  final def receive = processing(QueueStatus())

  final def processing(status: QueueStatus): Receive =
    handleWork(status, processing) orElse {

    case enq: Enqueue => (handleEnqueueUnderPressure(status) orElse {
      case Enqueue(workMessage, replyTo, setting) =>
        val newWork = Work(workMessage, setting.getOrElse(defaultWorkSettings))
        dispatchWorks(status.copy(workBuffer = status.workBuffer.enqueue(newWork)), processing)
        replyTo.foreach(_ ! WorkEnqueued)
    }: ReceiveEnqueue)(enq)

    case Retire(timeout) =>
      log.info("Queue commanded to retire")
      val newStatus = dispatchWorks(status, retiring)  //if there are still work and workers
      newStatus.queuedWorkers.foreach { (qw) =>
        qw ! NoWorkLeft
        context unwatch qw
      }
      delayedMsg(timeout, RetiringTimeout)
  }


  final def retiring(status: QueueStatus): Receive =
    if(status.workBuffer.isEmpty) {
      finish(status, s"Queue successfully retired")
      PartialFunction.empty //doesn't matter after finish, but is required by the api.
    } else handleWork(status, retiring) orElse {
      case Enqueue(_, replyTo, _) =>
        replyTo.getOrElse(sender) ! Retiring

      case RetiringTimeout => finish(status, "Forcefully retire after timed out")
  }

  private def finish(status: QueueStatus, withMessage: String): Unit = {
    log.info(withMessage + "- ${status.countOfWorkSent} work sent.")
    status.queuedWorkers.foreach( _ ! NoWorkLeft)
    context stop self
  }

  protected def handleEnqueueUnderPressure(status: QueueStatus): ReceiveEnqueue

  private def handleWork(status: QueueStatus, nextContext: QueueStatus => Receive): Receive = {
    case RequestWork(requester) =>
      context watch requester
      dispatchWorks(status.copy(queuedWorkers = status.queuedWorkers.enqueue(requester)), nextContext)

    case Unregister(worker) =>
      dispatchWorks(status.copy(queuedWorkers = status.queuedWorkers.filterNot(_ == worker)), nextContext)
      worker ! Unregistered

    case Terminated(worker) =>
      context become nextContext(status.copy(queuedWorkers = status.queuedWorkers.filter(_ != worker)))

    case Rejected(w, reason) =>
      log.info(s"work rejected, reason given by worker is '$reason'")
      dispatchWorks(status.copy(workBuffer = status.workBuffer.enqueue(w)), nextContext)

    case qs: QueryStatus => qs reply status

  }

  @tailrec
  protected final def dispatchWorks(status: QueueStatus, newContext: QueueStatus => Receive, dispatched: Int = 0): QueueStatus = {
    (for (
      (worker, queuedWorkers) <- status.queuedWorkers.dequeueOption;
      (work, workBuffer) <- status.workBuffer.dequeueOption
    ) yield {
      worker ! work
      context unwatch worker
      if(workBuffer.isEmpty) onQueuedWorkExhausted()
      status.copy(queuedWorkers = queuedWorkers, workBuffer = workBuffer, countOfWorkSent = status.countOfWorkSent + 1)
    }) match {
      case Some(newStatus) => dispatchWorks(newStatus, newContext, dispatched + 1) //actually in most cases, either works queue or workers queue is empty after one dispatch
      case None =>
        val lastHistory = status.bufferHistory
        val newEntry = BufferHistoryEntry(dispatched, status.workBuffer.length, LocalDateTime.now)
        val sampling = lastHistory.length > 1 && lastHistory.init.last.time.until(newEntry.time, ChronoUnit.MILLIS) < historySampleRateInMills //replacing the last entry if it's too close to the previous entry to achieve sampling while always retaining the latest status
        val updatedHistory = if(sampling) lastHistory.init :+ newEntry.aggregate(lastHistory.last) else status.bufferHistory.enqueueFinite(newEntry, bufferHistoryLength)
        context become newContext(status.copy(bufferHistory = updatedHistory))
        status
    }
  }

  def onQueuedWorkExhausted(): Unit = ()
}

case class QueueWithNaiveBackPressure(maxWorkBuffer: Int, defaultWorkSettings: WorkSetting) extends Queue {
  protected def bufferHistoryLength = 0
  protected def handleEnqueueUnderPressure(status: QueueStatus) : ReceiveEnqueue = {
    case Enqueue(workMessage, replyTo, _) if status.workBuffer.length >= maxWorkBuffer => replyTo.foreach(_ ! EnqueueRejected(workMessage, OverCapacity))
  }
}

private[queue] trait BackPressureControl {
  def settings: BackPressureSettings

  private[queue] def isOverCapacity(qs: QueueStatus, log: String => Unit = m => ()): Boolean = {
    val history = qs.bufferHistory
    if(history.isEmpty)
      false
    else if(history.last.numInBuffer >= settings.maxBufferSize){
      log("buffer overflowed" + settings.maxBufferSize)
      true
    } else if(history.last.numInBuffer == 0)
      false //avoid unnecessary math most of the time since there is no relevant history to calculate wait time.
    else {

      val expectedWaitTime = qs.avgDispatchDurationLowerBound.getOrElse(Duration.Zero) * qs.currentQueueLength

      val ret = expectedWaitTime > settings.thresholdForExpectedWaitTime
      if(ret) log(s"expected wait time ${expectedWaitTime.toMillis} ms is over threshold ${settings.thresholdForExpectedWaitTime}. queue size ${history.last.numInBuffer}")
      ret
    }
  }
}

case class QueueWithBackPressure(settings: BackPressureSettings,
                                 defaultWorkSettings: WorkSetting = WorkSetting()) extends Queue with BackPressureControl{

  protected val bufferHistoryLength = (settings.maxHistoryLength.toMillis / historySampleRateInMills).toInt
  assert(bufferHistoryLength > 5, s"max history length should be at least ${historySampleRateInMills * 5} ms" )

  protected def handleEnqueueUnderPressure(status: QueueStatus) : ReceiveEnqueue = {
    case Enqueue(workMessage, replyTo, _) if isOverCapacity(status, log.error) =>
      replyTo.foreach(_ ! EnqueueRejected(workMessage, OverCapacity))
  }

}

trait QueueWithoutBackPressure extends Queue {
  protected def bufferHistoryLength = 0
  protected def handleEnqueueUnderPressure(status: QueueStatus) : ReceiveEnqueue = PartialFunction.empty
}

case class DefaultQueue(defaultWorkSettings: WorkSetting) extends QueueWithoutBackPressure

class QueueOfIterator[T](private val iterator: Iterator[T], val defaultWorkSettings: WorkSetting) extends QueueWithoutBackPressure {
  override def preStart(): Unit = {
    super.preStart()
    onQueuedWorkExhausted()
  }

  override def onQueuedWorkExhausted(): Unit = {
    if(iterator.hasNext) {
      self ! Enqueue(iterator.next)
    } else {
      log.info("iterator queue completes")
      self ! Retire()
    }
  }
}

object Queue {

  case class RequestWork(requester: ActorRef)

  case class Enqueue(workMessage: Any, replyTo: Option[ActorRef] = None, workSettings: Option[WorkSetting] = None)
  object Enqueue {
    def apply(workMessage: Any, replyTo: ActorRef): Enqueue = Enqueue(workMessage, Some(replyTo))
  }
  case object WorkEnqueued
  case object Unregistered

  case class Unregister(worker: WorkerRef)

  case class EnqueueRejected(workMessage: Any, reason: Reason)

  object EnqueueRejected {
    sealed trait Reason
    case object OverCapacity extends Reason
  }

  case object Retiring
  case object NoWorkLeft
  case class Retire(timeout: FiniteDuration = 5.minutes)

  private case object RetiringTimeout

  trait QueueDispatchInfo {
    def avgDispatchDurationLowerBound: Option[Duration]
  }
  private[queue] case class QueueStatus(
                                    workBuffer: ScalaQueue[Work] = ScalaQueue.empty,
                                    queuedWorkers: ScalaQueue[ActorRef] = ScalaQueue.empty,
                                    countOfWorkSent: Int = 0,
                                    bufferHistory: Vector[BufferHistoryEntry] = Vector.empty) extends QueueDispatchInfo {

    lazy val relevantHistory: Vector[BufferHistoryEntry] = bufferHistory.takeRightWhile(_.numInBuffer > 0) //only take into account latest busy queue history

    /**
     * The lower bound of average duration it takes to dispatch one request， The reciprocal of it is the upper bound of dispatch speed.
     */
    lazy val avgDispatchDurationLowerBound: Option[Duration] = {
      if(relevantHistory.length >= 2) {
        val duration = relevantHistory.head.time.until(relevantHistory.last.time)
        val totalDispatched = relevantHistory.map(_.dispatched).sum
        Some(duration / Math.max(1, totalDispatched))
      } else None
    }

    def currentQueueLength = bufferHistory.lastOption.map(_.numInBuffer).getOrElse(0)
  }

  private[queue] case class BufferHistoryEntry(dispatched: Int, numInBuffer: Int, time: LocalDateTime) {
    def aggregate(that: BufferHistoryEntry) = copy(dispatched = dispatched + that.dispatched)
  }

  def ofIterator[T](iterator: Iterator[T], defaultWorkSetting: WorkSetting = WorkSetting()): Props = Props(new QueueOfIterator[T](iterator, defaultWorkSetting))
  
  def default(defaultWorkSetting: WorkSetting = WorkSetting()): Props = Props(new DefaultQueue(defaultWorkSetting))

  def withNaiveBackPressure(maxWorkBuffer: Int, workSetting: WorkSetting): Props = Props(QueueWithNaiveBackPressure(maxWorkBuffer, workSetting))

  def withBackPressure(backPressureSetting: BackPressureSettings,
                       defaultWorkSetting: WorkSetting): Props =
    Props(QueueWithBackPressure(backPressureSetting, defaultWorkSetting))

  /**
   *
   * @param maxBufferSize
   * @param thresholdForExpectedWaitTime
   * @param maxHistoryLength
   */

}

