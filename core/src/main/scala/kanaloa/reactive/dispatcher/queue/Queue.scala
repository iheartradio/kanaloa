package kanaloa.reactive.dispatcher.queue

import java.time.LocalDateTime

import akka.actor._
import kanaloa.reactive.dispatcher.ApiProtocol.{QueryStatus, WorkRejected}
import kanaloa.reactive.dispatcher.PerformanceSampler
import kanaloa.reactive.dispatcher.Types.QueueLength
import kanaloa.reactive.dispatcher.metrics.{MetricsCollector, Metric}
import kanaloa.reactive.dispatcher.queue.Queue.{Status, _}
import kanaloa.util.Java8TimeExtensions._
import kanaloa.util.MessageScheduler

import scala.annotation.tailrec
import scala.collection.immutable.{Queue ⇒ ScalaQueue}
import scala.concurrent.duration._

trait Queue extends Actor with ActorLogging with MessageScheduler {
  def defaultWorkSettings: WorkSettings
  def metricsCollector: ActorRef

  final def receive = processing(Status())

  metricsCollector ! Metric.WorkQueueLength(0)

  final def processing(status: Status): Receive =
    handleWork(status, processing) orElse {
      case e @ Enqueue(workMessage, sendAcks, sendResultsTo) ⇒
        val newWork = Work(workMessage, sendResultsTo, defaultWorkSettings)
        val newBuffer: ScalaQueue[Work] = status.workBuffer.enqueue(newWork)
        val newStatus: Status = dispatchWork(status.copy(workBuffer = newBuffer))
        metricsCollector ! Metric.WorkEnqueued
        if (sendAcks) {
          sender() ! WorkEnqueued
        }
        context become processing(newStatus)

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

  final def retiring(status: Status): Receive =
    if (status.workBuffer.isEmpty) {
      finish(status, s"Queue successfully retired")
      PartialFunction.empty //doesn't matter after finish, but is required by the api.
    } else handleWork(status, retiring) orElse {
      case e @ Enqueue(_, _, _) ⇒ sender() ! EnqueueRejected(e, Queue.EnqueueRejected.Retiring)
      case RetiringTimeout      ⇒ finish(status, "Forcefully retire after timed out")
    }

  private def finish(status: Status, withMessage: String): Unit = {
    log.info(withMessage + s"- ${status.countOfWorkSent} work sent.")
    status.queuedWorkers.foreach(_ ! NoWorkLeft)
    context stop self
  }

  private def handleWork(status: Status, nextContext: Status ⇒ Receive): Receive = {
    def dispatchWorkAndBecome(status: Status, newContext: Status ⇒ Receive): Unit = {
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
        log.debug(s"work rejected by worker, reason given by worker is '$reason'")
        dispatchWorkAndBecome(status.copy(workBuffer = status.workBuffer.enqueue(w)), nextContext)

      case qs: QueryStatus ⇒ qs reply status
    }
  }

  /**
   * Dispatch as many as possible work, so by the end either work queue or worker
   * queue should be empty.
   * Note that the workers left in the worker queue after dispatch are the only ones
   * that counts as idle workers.
   * @param status
   * @param dispatched
   * @param retiring
   * @return
   */
  @tailrec
  protected final def dispatchWork(status: Status, dispatched: Int = 0, retiring: Boolean = false): Status = {
    if (status.workBuffer.isEmpty && !status.queuedWorkers.isEmpty && !retiring) onQueuedWorkExhausted()
    (for (
      (worker, queuedWorkers) ← status.queuedWorkers.dequeueOption;
      (work, workBuffer) ← status.workBuffer.dequeueOption
    ) yield {
      worker ! work
      context unwatch worker
      status.copy(queuedWorkers = queuedWorkers, workBuffer = workBuffer, countOfWorkSent = status.countOfWorkSent + 1)
    }) match {
      case Some(newStatus) ⇒ dispatchWork(newStatus, dispatched + 1, retiring) //actually in most cases, either works queue or workers queue is empty after one dispatch
      case None ⇒
        metricsCollector ! PerformanceSampler.DispatchResult(status.queuedWorkers.length, QueueLength(status.workBuffer.length))
        status
    }
  }

  def onQueuedWorkExhausted(): Unit = ()
}

case class DefaultQueue(
  defaultWorkSettings: WorkSettings,
  metricsCollector:    ActorRef
) extends Queue

class QueueOfIterator(
  private val iterator:    Iterator[_],
  val defaultWorkSettings: WorkSettings,
  val metricsCollector:    ActorRef,
  sendResultsTo:           Option[ActorRef] = None
) extends Queue {
  import QueueOfIterator._

  val enqueuer = context.actorOf(enqueueerProps(iterator, sendResultsTo, self))

  override def onQueuedWorkExhausted(): Unit = enqueuer ! EnqueueMore
}

object QueueOfIterator {
  def props(
    iterator:            Iterator[_],
    defaultWorkSettings: WorkSettings,
    metricsCollector:    ActorRef,
    sendResultsTo:       Option[ActorRef] = None
  ): Props =
    Props(new QueueOfIterator(iterator, defaultWorkSettings, metricsCollector, sendResultsTo)).withDeploy(Deploy.local)

  private case object EnqueueMore

  private class Enqueuer(iterator: Iterator[_], sendResultsTo: Option[ActorRef], queue: ActorRef) extends Actor with ActorLogging {
    def receive = {
      case EnqueueMore ⇒
        if (iterator.hasNext) {
          queue ! Enqueue(iterator.next, false, sendResultsTo)
        } else {
          log.debug("Iterator queue completes.")
          queue ! Retire()
        }
    }
  }

  private def enqueueerProps(iterator: Iterator[_], sendResultsTo: Option[ActorRef], queue: ActorRef): Props = Props(new Enqueuer(iterator, sendResultsTo, queue)).withDeploy(Deploy.local)
}

object Queue {

  case class RequestWork(requester: ActorRef)

  /**
   * Enqueue a message. If the message is enqueued successfully, a [[kanaloa.reactive.dispatcher.queue.Queue.WorkEnqueued]]
   * is sent to the sender if `sendAcks` is true.
   * Any results will be sent to the `replyTo` actor.  If the work is rejected, a [[WorkRejected]] is sent to the sender,
   * regardless of the value of `sendAcks`.
   *
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
   *
   * @param message  Rejected [[Enqueue]] message
   * @param reason  Reason for rejection
   */
  case class EnqueueRejected(message: Enqueue, reason: EnqueueRejected.Reason)

  object EnqueueRejected {
    sealed trait Reason
    case object Retiring extends Reason
  }

  case object NoWorkLeft
  case class Retire(timeout: FiniteDuration = 5.minutes)

  private case object RetiringTimeout

  protected[queue] case class Status(
    workBuffer:      ScalaQueue[Work]             = ScalaQueue.empty,
    queuedWorkers:   ScalaQueue[ActorRef]         = ScalaQueue.empty,
    countOfWorkSent: Long                         = 0,
    dispatchHistory: Vector[DispatchHistoryEntry] = Vector.empty
  )

  private[queue] case class DispatchHistoryEntry(dispatched: Int, queueLength: Int, waitingWorkers: Int, time: LocalDateTime) {
    def aggregate(that: DispatchHistoryEntry) = copy(dispatched = dispatched + that.dispatched)

    def allWorkerOccupied = queueLength > 0 || waitingWorkers == 0
  }

  def ofIterable(
    iterable:           Iterable[_],
    metricsCollector:   ActorRef,
    defaultWorkSetting: WorkSettings     = WorkSettings(),
    sendResultsTo:      Option[ActorRef] = None
  ): Props =
    QueueOfIterator.props(iterable.iterator, defaultWorkSetting, metricsCollector, sendResultsTo).withDeploy(Deploy.local)

  def ofIterator(
    iterator:           Iterator[_],
    metricsCollector:   ActorRef,
    defaultWorkSetting: WorkSettings     = WorkSettings(),
    sendResultsTo:      Option[ActorRef] = None
  ): Props =
    QueueOfIterator.props(iterator, defaultWorkSetting, metricsCollector, sendResultsTo).withDeploy(Deploy.local)

  def default(
    metricsCollector:   ActorRef,
    defaultWorkSetting: WorkSettings = WorkSettings()
  ): Props =
    Props(new DefaultQueue(defaultWorkSetting, metricsCollector)).withDeploy(Deploy.local)

}

