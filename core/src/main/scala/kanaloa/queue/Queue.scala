package kanaloa.queue

import akka.actor._
import kanaloa.ApiProtocol.{QueryStatus, WorkRejected}
import kanaloa.WorkerPoolSampler
import kanaloa.Types.QueueLength
import kanaloa.metrics.Metric
import kanaloa.queue.Queue._
import kanaloa.util.MessageScheduler, kanaloa.util.AnyEq._

import scala.annotation.tailrec
import scala.collection.immutable.{Queue ⇒ ScalaQueue}
import scala.concurrent.duration._

trait Queue[T] extends Actor with ActorLogging with MessageScheduler {
  def defaultWorkSettings: WorkSettings
  def metricsCollector: ActorRef

  val initialState = InternalState()
  final def receive = processing(initialState)

  metricsCollector ! statusOf(initialState)

  val reportScheduler = {
    import context.dispatcher
    val reportInterval = 5.seconds
    context.system.scheduler.schedule(reportInterval, reportInterval, self, SubmitReport) //this is needed because statsD metrics report is not reliable
  }

  override def postStop(): Unit = {
    reportScheduler.cancel()
  }

  final def processing(state: InternalState): Receive =
    handleWork(state, false) orElse {
      case e @ Enqueue(workMessage: T, sendAcks, sendResultsTo) ⇒
        val newWork = Work(workMessage, sendResultsTo, defaultWorkSettings)
        val newBuffer: ScalaQueue[Work[T]] = state.workBuffer.enqueue(newWork)
        val newStatus: InternalState = dispatchWork(state.copy(workBuffer = newBuffer))
        if (sendAcks) {
          sender() ! WorkEnqueued
        }
        context become processing(newStatus)

      case Retire(timeout) ⇒
        log.debug("Queue commanded to retire")
        val newStatus = dispatchWork(state, retiring = true)
        context become retiring(newStatus)
        newStatus.queuedWorkers.foreach { qw ⇒
          qw ! NoWorkLeft
          context unwatch qw
        }
        delayedMsg(timeout, RetiringTimeout)

      case SubmitReport ⇒ metricsCollector ! statusOf(state)
    }

  final def retiring(state: InternalState): Receive =
    if (state.workBuffer.isEmpty) {
      finish(state, s"Queue successfully retired")
      PartialFunction.empty //doesn't matter after finish, but is required by the api.
    } else handleWork(state, true) orElse {
      case e @ Enqueue(_, _, _) ⇒ sender() ! EnqueueRejected(e, Queue.EnqueueRejected.Retiring)
      case RetiringTimeout ⇒ finish(state, "Forcefully 122" +
        "retire after timed out")
    }

  private def finish(state: InternalState, withMessage: String): Unit = {
    log.info(withMessage + s"- ${state.countOfWorkSent} work sent.")
    state.queuedWorkers.foreach(_ ! NoWorkLeft)
    context stop self
  }

  private def handleWork(state: InternalState, isRetiring: Boolean): Receive = {
    def dispatchWorkAndBecome(state: InternalState): InternalState = {
      val newState = dispatchWork(state)
      next(newState)
      newState
    }

    def next(newState: InternalState): Unit =
      context become (if (isRetiring) retiring(newState) else processing(newState))

    {
      case RequestWork(requester) ⇒
        context watch requester
        val newState = dispatchWorkAndBecome(state.copy(queuedWorkers = state.queuedWorkers.enqueue(requester)))
        if (newState.workBuffer.isEmpty && !newState.queuedWorkers.isEmpty && !isRetiring)
          onQueuedWorkExhausted()

      case Unregister(worker) ⇒
        dispatchWorkAndBecome(state.copy(queuedWorkers = state.queuedWorkers.filterNot(_ === worker)))
        worker ! Unregistered

      case Terminated(worker) ⇒
        next(state.copy(queuedWorkers = state.queuedWorkers.filter(_ != worker)))

      case r: Rejected[T] ⇒
        log.debug(s"work rejected by worker, reason given by worker is '${r.reason}'")
        dispatchWorkAndBecome(state.copy(workBuffer = state.workBuffer.enqueue(r.work)))
    }
  }

  /**
   * Dispatch as many as possible work, so by the end either work queue or worker
   * queue should be empty.
   * Note that the workers left in the worker queue after dispatch are the only ones
   * that counts as idle workers.
   *
   * @param state
   * @param dispatched
   * @param retiring
   * @return
   */
  @tailrec
  protected final def dispatchWork(state: InternalState, dispatched: Int = 0, retiring: Boolean = false): InternalState = {

    (for {
      (worker, queuedWorkers) ← state.queuedWorkers.dequeueOption
      (work, workBuffer) ← state.workBuffer.dequeueOption
    } yield {
      worker ! work
      context unwatch worker
      state.copy(queuedWorkers = queuedWorkers, workBuffer = workBuffer, countOfWorkSent = state.countOfWorkSent + 1)
    }) match {
      case Some(newState) ⇒ dispatchWork(newState, dispatched + 1, retiring) //actually in most cases, either works queue or workers queue is empty after one dispatch
      case None ⇒
        val newStatus = statusOf(state)
        metricsCollector ! DispatchReport(newStatus, dispatched)
        state
    }
  }

  def fullyUtilized(state: InternalState): Boolean
  def onQueuedWorkExhausted(): Unit = ()
  private def statusOf(state: InternalState): Queue.Status =
    Queue.Status(state.queuedWorkers.length, QueueLength(state.workBuffer.length), fullyUtilized(state))

  protected case class InternalState(
    workBuffer:      ScalaQueue[Work[T]]  = ScalaQueue.empty,
    queuedWorkers:   ScalaQueue[ActorRef] = ScalaQueue.empty,
    countOfWorkSent: Long                 = 0
  )

}

case class DefaultQueue[T](
  defaultWorkSettings: WorkSettings,
  metricsCollector:    ActorRef
) extends Queue[T] {
  def fullyUtilized(state: InternalState): Boolean = state.queuedWorkers.length == 0 && state.workBuffer.length > 0
}

class QueueOfIterator[T](
  private val iterator:    Iterator[T],
  val defaultWorkSettings: WorkSettings,
  val metricsCollector:    ActorRef,
  sendResultsTo:           Option[ActorRef] = None
) extends Queue[T] {
  import QueueOfIterator._

  val enqueuer = context.actorOf(enqueueerProps(iterator, sendResultsTo, self, metricsCollector))

  /**
   * Determines if a state indicates the workers pool are fully utilized.
   * This is different from the default pushing [[DefaultQueue]]
   * for a QueueOfIterator, it only gets work when there is at least one queued worker,
   * which means there is a significant chance a second worker comes in before
   * the first worker gets work. This number is still a bit arbitrary though.
   * Obviously we still have to chose an arbitrary number as the threshold of queued workers
   * with which we deem the queue as partially utilized.
   * Todo: Right now we lack the insight of how to set this up correctly so I'd rather have
   * it hard coded for now than allowing our users to tweak it without giving them any guidance
   *
   * @param state
   * @return
   */
  def fullyUtilized(state: InternalState): Boolean = state.queuedWorkers.length <= 2

  override def onQueuedWorkExhausted(): Unit = enqueuer ! EnqueueMore
}

object QueueOfIterator {
  def props[T](
    iterator:            Iterator[T],
    defaultWorkSettings: WorkSettings,
    metricsCollector:    ActorRef,
    sendResultsTo:       Option[ActorRef] = None
  ): Props =
    Props(new QueueOfIterator(
      iterator,
      defaultWorkSettings,
      metricsCollector,
      sendResultsTo
    )).withDeploy(Deploy.local)

  private case object EnqueueMore

  private class Enqueuer[T](
    iterator:         Iterator[T],
    sendResultsTo:    Option[ActorRef],
    queue:            ActorRef,
    metricsCollector: ActorRef
  ) extends Actor with ActorLogging {
    def receive = {
      case EnqueueMore ⇒
        if (iterator.hasNext) {
          metricsCollector ! Metric.WorkReceived
          queue ! Enqueue(iterator.next, false, sendResultsTo)
        } else {
          log.debug("Iterator queue completes.")
          queue ! Retire()
        }
    }
  }

  private def enqueueerProps[T](
    iterator:         Iterator[T],
    sendResultsTo:    Option[ActorRef],
    queue:            ActorRef,
    metricsCollector: ActorRef
  ): Props = Props(new Enqueuer(iterator, sendResultsTo, queue, metricsCollector)).withDeploy(Deploy.local)

}

object Queue {

  case class RequestWork(requester: ActorRef)
  private case object SubmitReport
  /**
   * Enqueue a message. If the message is enqueued successfully, a [[kanaloa.queue.Queue.WorkEnqueued]]
   * is sent to the sender if `sendAcks` is true.
   * Any results will be sent to the `replyTo` actor.  If the work is rejected, a [[WorkRejected]] is sent to the sender,
   * regardless of the value of `sendAcks`.
   *
   * @param workMessage The message to enqueue
   * @param sendAcks Send ack messages.  This does not control [[WorkRejected]] messages, which are sent regardless for backpressure.
   * @param sendResultsTo Actor which can optionally receive responses from downstream backends.
   */
  case class Enqueue[T](workMessage: T, sendAcks: Boolean = false, sendResultsTo: Option[ActorRef] = None)

  case object WorkEnqueued
  case object Unregistered

  case class Unregister(worker: WorkerRef)

  /**
   * Sent back to a sender of an [[Enqueue]] message if a [[Queue]] rejected the [[Enqueue]] message
   *
   * @param message  Rejected [[Enqueue]] message
   * @param reason  Reason for rejection
   */
  case class EnqueueRejected[T](message: Enqueue[T], reason: EnqueueRejected.Reason)

  object EnqueueRejected {
    sealed trait Reason
    case object Retiring extends Reason
  }

  case object NoWorkLeft
  case class Retire(timeout: FiniteDuration = 5.minutes)

  private case object RetiringTimeout

  /**
   * Public status of the queue
   *
   * @param idleWorkers workers that are waiting for work
   * @param queueLength work in the queue waiting to be picked up by workers
   * @param fullyUtilized are all workers in the worker pool utilized
   */
  case class Status(idleWorkers: Int, queueLength: QueueLength, fullyUtilized: Boolean)
  case class DispatchReport(status: Status, dispatched: Int)

  def ofIterable[T](
    iterable:           Iterable[T],
    metricsCollector:   ActorRef,
    defaultWorkSetting: WorkSettings     = WorkSettings(),
    sendResultsTo:      Option[ActorRef] = None
  ): Props =
    QueueOfIterator.props(iterable.iterator, defaultWorkSetting, metricsCollector, sendResultsTo).withDeploy(Deploy.local)

  def ofIterator[T](
    iterator:           Iterator[T],
    metricsCollector:   ActorRef,
    defaultWorkSetting: WorkSettings     = WorkSettings(),
    sendResultsTo:      Option[ActorRef] = None
  ): Props =
    QueueOfIterator.props(iterator, defaultWorkSetting, metricsCollector, sendResultsTo).withDeploy(Deploy.local)

  def default[T](
    metricsCollector:   ActorRef,
    defaultWorkSetting: WorkSettings = WorkSettings()
  ): Props =
    Props(new DefaultQueue[T](defaultWorkSetting, metricsCollector)).withDeploy(Deploy.local)

}
