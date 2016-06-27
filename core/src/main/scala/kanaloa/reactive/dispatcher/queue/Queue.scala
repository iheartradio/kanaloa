package kanaloa.reactive.dispatcher.queue

import java.time.LocalDateTime

import akka.actor._
import kanaloa.reactive.dispatcher.ApiProtocol.{QueryStatus, WorkRejected}
import kanaloa.reactive.dispatcher.metrics.{Metric, MetricsCollector, NoOpMetricsCollector}
import kanaloa.reactive.dispatcher.queue.Queue.{QueueStatus, _}
import kanaloa.util.FiniteCollection._
import kanaloa.util.Java8TimeExtensions._
import kanaloa.util.MessageScheduler

import scala.annotation.tailrec
import scala.collection.immutable.{Queue ⇒ ScalaQueue}
import scala.concurrent.duration._

trait Queue extends Actor with ActorLogging with MessageScheduler {
  def dispatchHistorySettings: DispatchHistorySettings
  def defaultWorkSettings: WorkSettings
  def metricsCollector: MetricsCollector
  val dispatchHistoryLength = (dispatchHistorySettings.maxHistoryLength / dispatchHistorySettings.historySampleRate).toInt
  assert(dispatchHistoryLength > 5, s"max history length should be at least ${dispatchHistorySettings.historySampleRate * 5}")

  final def receive = processing(QueueStatus())

  metricsCollector.send(Metric.WorkQueueLength(0))

  final def processing(status: QueueStatus): Receive =
    handleWork(status, processing) orElse {
      case e @ Enqueue(workMessage, sendAcks, sendResultsTo) ⇒
        if (checkOverCapacity(status)) {
          log.warning("At capacity, rejecting message [{}]", workMessage)
          sender() ! EnqueueRejected(e, Queue.EnqueueRejected.OverCapacity)
          metricsCollector.send(Metric.EnqueueRejected)
        } else {
          val newWork = Work(workMessage, sendResultsTo, defaultWorkSettings)
          val newBuffer: ScalaQueue[Work] = status.workBuffer.enqueue(newWork)
          val newStatus: QueueStatus = dispatchWork(status.copy(workBuffer = newBuffer))
          metricsCollector.send(Metric.WorkEnqueued)
          if (sendAcks) {
            sender() ! WorkEnqueued
          }
          context become processing(newStatus)
        }

      case Retire(timeout) ⇒
        log.debug("Queue commanded to retire")
        val newStatus = dispatchWork(status, retiring = true)
        context become retiring(newStatus)
        newStatus.queuedWorkers.foreach { qw ⇒
          qw ! NoWorkLeft
          context unwatch qw
        }
        delayedMsg(timeout, RetiringTimeout)
    }

  final def retiring(status: QueueStatus): Receive =
    if (status.workBuffer.isEmpty) {
      finish(status, s"Queue successfully retired")
      PartialFunction.empty //doesn't matter after finish, but is required by the api.
    } else handleWork(status, retiring) orElse {
      case Enqueue(_, _, _) ⇒ sender() ! Retiring
      case RetiringTimeout  ⇒ finish(status, "Forcefully retire after timed out")
    }

  private def finish(status: QueueStatus, withMessage: String): Unit = {
    log.info(withMessage + s"- ${status.countOfWorkSent} work sent.")
    status.queuedWorkers.foreach(_ ! NoWorkLeft)
    context stop self
  }

  /**
   * Check if currently over capacity
   * @param qs
   * @return true if over capacity
   */
  protected def checkOverCapacity(qs: QueueStatus): Boolean

  private def handleWork(status: QueueStatus, nextContext: QueueStatus ⇒ Receive): Receive = {
    def dispatchWorkAndBecome(status: QueueStatus, newContext: QueueStatus ⇒ Receive): Unit = {
      val newStatus = dispatchWork(status)
      context become newContext(newStatus)
    }

    {
      case RequestWork(requester) ⇒
        context watch requester
        dispatchWorkAndBecome(status.copy(queuedWorkers = status.queuedWorkers.enqueue(requester)), nextContext)

      case Unregister(worker) ⇒
        dispatchWorkAndBecome(status.copy(queuedWorkers = status.queuedWorkers.filterNot(_ == worker)), nextContext)
        worker ! Unregistered

      case Terminated(worker) ⇒
        context become nextContext(status.copy(queuedWorkers = status.queuedWorkers.filter(_ != worker)))

      case Rejected(w, reason) ⇒
        log.debug(s"work rejected, reason given by worker is '$reason'")
        dispatchWorkAndBecome(status.copy(workBuffer = status.workBuffer.enqueue(w)), nextContext)

      case qs: QueryStatus ⇒ qs reply status
    }
  }

  @tailrec
  protected final def dispatchWork(status: QueueStatus, dispatched: Int = 0, retiring: Boolean = false): QueueStatus = {
    def updatedHistory = {
      val lastHistory = status.dispatchHistory
      val newEntry = DispatchHistoryEntry(dispatched, status.workBuffer.length, status.queuedWorkers.length, LocalDateTime.now)
      val sampling = lastHistory.length > 1 && lastHistory.init.last.time.until(newEntry.time) < dispatchHistorySettings.historySampleRate //whether to replace the last entry if it's too close to the previous entry to achieve sampling while always retaining the latest status
      if (sampling)
        lastHistory.init :+ newEntry.aggregate(lastHistory.last)
      else
        status.dispatchHistory.enqueueFinite(newEntry, dispatchHistoryLength)
    }

    (for (
      (worker, queuedWorkers) ← status.queuedWorkers.dequeueOption;
      (work, workBuffer) ← status.workBuffer.dequeueOption
    ) yield {
      worker ! work
      context unwatch worker
      if (workBuffer.isEmpty && !retiring) onQueuedWorkExhausted()
      status.copy(queuedWorkers = queuedWorkers, workBuffer = workBuffer, countOfWorkSent = status.countOfWorkSent + 1)
    }) match {
      case Some(newStatus) ⇒ dispatchWork(newStatus, dispatched + 1, retiring) //actually in most cases, either works queue or workers queue is empty after one dispatch
      case None ⇒
        metricsCollector.send(Metric.WorkQueueLength(status.workBuffer.length))
        if (dispatchHistoryLength > 0)
          status.copy(dispatchHistory = updatedHistory)
        else status
    }
  }

  def onQueuedWorkExhausted(): Unit = ()
}

case class QueueWithBackPressure(
  dispatchHistorySettings: DispatchHistorySettings,
  backPressureSettings:    BackPressureSettings,
  defaultWorkSettings:     WorkSettings            = WorkSettings(),
  metricsCollector:        MetricsCollector        = NoOpMetricsCollector
) extends Queue {

  def checkOverCapacity(qs: QueueStatus): Boolean =
    if (qs.currentQueueLength >= backPressureSettings.maxBufferSize) {
      log.warning("buffer overflowed " + backPressureSettings.maxBufferSize)
      true
    } else {
      val expectedWaitTime = qs.avgDispatchDurationLowerBoundWhenFullyUtilized.getOrElse(Duration.Zero) * qs.currentQueueLength
      metricsCollector.send(Metric.WorkQueueExpectedWaitTime(expectedWaitTime))

      val ret = expectedWaitTime > backPressureSettings.thresholdForExpectedWaitTime
      if (ret) log.warning(s"expected wait time ${expectedWaitTime.toMillis} ms is over threshold ${backPressureSettings.thresholdForExpectedWaitTime}. queue size ${qs.currentQueueLength}")
      ret
    }

}

trait QueueWithoutBackPressure extends Queue {
  def checkOverCapacity(qs: QueueStatus) = false
}

case class DefaultQueue(
  dispatchHistorySettings: DispatchHistorySettings,
  defaultWorkSettings:     WorkSettings,
  metricsCollector:        MetricsCollector        = NoOpMetricsCollector
) extends QueueWithoutBackPressure

class QueueOfIterator(
  private val iterator:        Iterator[_],
  val dispatchHistorySettings: DispatchHistorySettings,
  val defaultWorkSettings:     WorkSettings,
  sendResultsTo:               Option[ActorRef]        = None,
  val metricsCollector:        MetricsCollector        = NoOpMetricsCollector
) extends QueueWithoutBackPressure {
  private case object EnqueueMore
  private class Enqueuer extends Actor {
    def receive = {
      case EnqueueMore ⇒
        if (iterator.hasNext) {
          context.parent ! Enqueue(iterator.next, false, sendResultsTo)
        } else {
          log.debug("Iterator queue completes.")
          context.parent ! Retire()
        }
    }
  }

  val enqueuer = context.actorOf(Props(new Enqueuer))

  override def preStart(): Unit = {
    super.preStart()
    onQueuedWorkExhausted()
  }

  override def onQueuedWorkExhausted(): Unit = enqueuer ! EnqueueMore
}

object QueueOfIterator {
  def props(
    iterator:                Iterator[_],
    dispatchHistorySettings: DispatchHistorySettings,
    defaultWorkSettings:     WorkSettings,
    sendResultsTo:           Option[ActorRef]        = None,
    metricsCollector:        MetricsCollector        = NoOpMetricsCollector
  ): Props =
    Props(new QueueOfIterator(iterator, dispatchHistorySettings, defaultWorkSettings, sendResultsTo, metricsCollector))
}

object Queue {

  case class RequestWork(requester: ActorRef)

  /**
   * Enqueue a message. If the message is enqueued successfully, a [[kanaloa.reactive.dispatcher.queue.Queue.WorkEnqueued]]
   * is sent to the sender if `sendAcks` is true.
   * Any results will be sent to the `replyTo` actor.  If the work is rejected, a [[WorkRejected]] is sent to the sender,
   * regardless of the value of `sendAcks`.
   * @param workMessage The message to enqueue
   * @param sendAcks Send ack messages.  This does not control [[WorkRejected]] messages, which are sent regardless for backpressure.
   * @param sendResultsTo Actor which can optionally receive responses from downstream backends.
   */
  case class Enqueue(workMessage: Any, sendAcks: Boolean = false, sendResultsTo: Option[ActorRef] = None)

  case object WorkEnqueued
  case object Unregistered

  case class Unregister(worker: WorkerRef)

  /**
   * Sent back to a sender of an [[Enqueue]] message if a [[Queue]] rejected the [[Enqueue]] message
   * @param message  Rejected [[Enqueue]] message
   * @param reason  Reason for rejection
   */
  case class EnqueueRejected(message: Enqueue, reason: EnqueueRejected.Reason)

  object EnqueueRejected {
    sealed trait Reason
    case object OverCapacity extends Reason
  }

  case object Retiring
  case object NoWorkLeft
  case class Retire(timeout: FiniteDuration = 5.minutes)

  private case object RetiringTimeout

  trait QueueDispatchInfo {
    /**
     * The average duration it takes to dispatch a message, this is a lower bound (actual could be slower than this).
     * Also it only measures when the all the workers are fully utilized.
     */
    def avgDispatchDurationLowerBoundWhenFullyUtilized: Option[Duration]
  }

  protected[queue] case class QueueStatus(
    workBuffer:      ScalaQueue[Work]             = ScalaQueue.empty,
    queuedWorkers:   ScalaQueue[ActorRef]         = ScalaQueue.empty,
    countOfWorkSent: Long                         = 0,
    dispatchHistory: Vector[DispatchHistoryEntry] = Vector.empty
  ) extends QueueDispatchInfo {

    lazy val relevantHistory: Vector[DispatchHistoryEntry] = dispatchHistory.takeRightWhile(_.allWorkerOccupied) //only take into account latest busy queue history

    /**
     * The lower bound of average duration it takes to dispatch one request， The reciprocal of it is the upper bound of dispatch speed.
     */
    lazy val avgDispatchDurationLowerBoundWhenFullyUtilized: Option[Duration] = {
      if (relevantHistory.length >= 2) {
        val duration = relevantHistory.head.time.until(relevantHistory.last.time)
        val totalDispatched = relevantHistory.map(_.dispatched).sum
        Some(duration / Math.max(1, totalDispatched))
      } else None
    }

    lazy val currentQueueLength = dispatchHistory.lastOption.map(_.queueLength).getOrElse(0)
  }

  private[queue] case class DispatchHistoryEntry(dispatched: Int, queueLength: Int, waitingWorkers: Int, time: LocalDateTime) {
    def aggregate(that: DispatchHistoryEntry) = copy(dispatched = dispatched + that.dispatched)

    def allWorkerOccupied = queueLength > 0 || waitingWorkers == 0
  }

  def ofIterable(
    iterable:                Iterable[_],
    dispatchHistorySettings: DispatchHistorySettings,
    defaultWorkSetting:      WorkSettings            = WorkSettings(),
    sendResultsTo:           Option[ActorRef]        = None,
    metricsCollector:        MetricsCollector        = NoOpMetricsCollector
  ): Props =
    QueueOfIterator.props(iterable.iterator, dispatchHistorySettings, defaultWorkSetting, sendResultsTo, metricsCollector)

  def ofIterator(
    iterator:                Iterator[_],
    dispatchHistorySettings: DispatchHistorySettings,
    defaultWorkSetting:      WorkSettings            = WorkSettings(),
    sendResultsTo:           Option[ActorRef]        = None,
    metricsCollector:        MetricsCollector        = NoOpMetricsCollector
  ): Props =
    QueueOfIterator.props(iterator, dispatchHistorySettings, defaultWorkSetting, sendResultsTo, metricsCollector)

  def default(
    dispatchHistorySettings: DispatchHistorySettings = DispatchHistorySettings(),
    defaultWorkSetting:      WorkSettings            = WorkSettings(),
    metricsCollector:        MetricsCollector        = NoOpMetricsCollector
  ): Props =
    Props(new DefaultQueue(dispatchHistorySettings, defaultWorkSetting, metricsCollector))

  def withBackPressure(
    dispatchHistorySettings: DispatchHistorySettings,
    backPressureSetting:     BackPressureSettings,
    defaultWorkSettings:     WorkSettings            = WorkSettings(),
    metricsCollector:        MetricsCollector        = NoOpMetricsCollector
  ): Props =
    Props(QueueWithBackPressure(dispatchHistorySettings, backPressureSetting, defaultWorkSettings, metricsCollector))

}

