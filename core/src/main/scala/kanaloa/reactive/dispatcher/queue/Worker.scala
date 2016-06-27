package kanaloa.reactive.dispatcher.queue

import java.time.LocalDateTime

import akka.actor._
import akka.routing.Routee
import kanaloa.reactive.dispatcher.ApiProtocol.{QueryStatus, WorkFailed, WorkTimedOut}
import kanaloa.reactive.dispatcher.queue.Queue.{NoWorkLeft, RequestWork, Unregister, Unregistered}
import kanaloa.reactive.dispatcher.queue.QueueProcessor.WorkCompleted
import kanaloa.reactive.dispatcher.queue.Worker._
import kanaloa.reactive.dispatcher.{Backend, ResultChecker}
import kanaloa.util.Java8TimeExtensions._
import kanaloa.util.MessageScheduler

import scala.concurrent.duration._

trait Worker extends Actor with ActorLogging with MessageScheduler {

  protected def backend: Backend //actor who really does the work
  protected val queue: ActorRef
  protected def monitor: ActorRef = context.parent

  def receive = starting(None, true)

  context watch queue

  private var routee: ActorRef = null

  var delayBeforeNextWork: Option[FiniteDuration] = None

  //testing purpose only
  private[queue] def getRoutee: ActorRef = routee

  override def preStart(): Unit = retrieveRoutee()

  def retrieveRoutee(): Unit = {
    import context.dispatcher
    backend(context).foreach { ref ⇒
      self ! RouteeReceived(ref)
    }
  }

  def starting(work: Option[(Work, ActorRef)], askWorkImmediately: Boolean): Receive = whileWaiting(Starting) orElse {
    case RouteeReceived(r) ⇒
      routee = r
      context watch r
      context become waitingForWork
      if (askWorkImmediately && work.isEmpty) askMoreWork(None)
      work.foreach {
        case (workMsg, workSender) ⇒ self.tell(workMsg, workSender)
      }

    case work: Work ⇒ //this only happens on restarting with new routee because the old one died.
      context become starting(Some(work, sender), false)

    case Worker.Retire ⇒
      work.foreach {
        case (workMsg, workSender) ⇒ workSender ! Rejected(workMsg, "retiring") //this happens only when a Retire is sent during a restart with new routee
      }
      finish()

  }

  val waitingForWork: Receive =
    whileWaiting(Idle) orElse {

      case work: Work ⇒ sendWorkToDelegatee(work, 0)

      case Terminated(r) if r == routee ⇒
        context become starting(None, false)
        retrieveRoutee()

      case Worker.Retire ⇒
        queue ! Unregister(self)
        context become retiring(None)
    }

  def whileWaiting(currentStatus: WorkerStatus): Receive = {
    case NoWorkLeft ⇒ finish()

    case Hold(period) ⇒
      delayBeforeNextWork = Some(period)

    case qs: QueryStatus     ⇒ qs reply currentStatus

    case Terminated(`queue`) ⇒ finish()

  }

  def finish(): Unit = context stop self

  def working(outstanding: Outstanding): Receive = ({
    case Hold(period) ⇒
      delayBeforeNextWork = Some(period)

    case Terminated(`queue`) ⇒ context become retiring(Some(outstanding))

    case Terminated(r) if r == routee ⇒
      outstanding.fail(WorkFailed(s"due ${routee.path} is terminated"))
      context become starting(None, false)
      retrieveRoutee()

    case qs: QueryStatus ⇒ qs reply Working

    case Worker.Retire   ⇒ context become retiring(Some(outstanding))

  }: Receive).orElse(waitingResult(outstanding, false, delayBeforeNextWork))

  def retiring(outstanding: Option[Outstanding]): Receive = ({
    case Terminated(_)   ⇒ //ignore when retiring
    case qs: QueryStatus ⇒ qs reply Retiring
    case Unregistered    ⇒ finish()
    case Retire          ⇒ //already retiring
    case Hold(period)    ⇒ //ignore

  }: Receive) orElse (
    if (outstanding.isDefined)
      waitingResult(outstanding.get, true, None)
    else {
      case w: Work ⇒
        sender ! Rejected(w, "Retiring")
        finish()
    }
  )

  def waitingResult(
    outstanding:         Outstanding,
    isRetiring:          Boolean,
    delayBeforeNextWork: Option[FiniteDuration]
  ): Receive = {
    val handleResult: Receive =
      (resultChecker orElse ({

        case m ⇒ Left(s"Unmatched Result '${descriptionOf(m)}' from the backend service, update your ResultChecker if you want to prevent it from being treated as an error.")

      }: ResultChecker)).andThen[Unit] {

        case Right(result) ⇒
          outstanding.success(result)
          if (isRetiring) finish() else {
            askMoreWork(delayBeforeNextWork)
          }
        case Left(e) ⇒
          log.warning(s"Error $e returned by routee in regards to running work $outstanding")
          retryOrAbandon(outstanding, isRetiring, e, delayBeforeNextWork)
      }

    ({
      case RouteeTimeout ⇒
        log.warning(s"Routee ${routee.path} timed out after ${outstanding.work.settings.timeout} work ${outstanding.work.messageToDelegatee} abandoned")
        outstanding.timeout()

        if (isRetiring) finish() else {
          askMoreWork(delayBeforeNextWork)
        }
      case w: Work ⇒ sender ! Rejected(w, "busy") //just in case

    }: Receive) orElse handleResult

  }

  private def retryOrAbandon(
    outstanding:         Outstanding,
    isRetiring:          Boolean,
    error:               Any,
    delayBeforeNextWork: Option[FiniteDuration]
  ): Unit = {
    outstanding.cancel()
    if (outstanding.retried < outstanding.work.settings.retry && delayBeforeNextWork.isEmpty) {
      log.debug(s"Retry work $outstanding")
      sendWorkToDelegatee(outstanding.work, outstanding.retried + 1)
    } else {
      def message = {
        val retryMessage = if (outstanding.retried > 0) s"after ${outstanding.retried + 1} try(s)" else ""
        s"Processing of '${outstanding.workDescription}' failed $retryMessage"
      }
      log.warning(s"$message, work abandoned")
      outstanding.fail(WorkFailed(message + s" due to ${descriptionOf(error)}"))
      if (isRetiring) finish()
      else
        askMoreWork(delayBeforeNextWork)
    }
  }

  private def sendWorkToDelegatee(work: Work, retried: Int): Unit = {
    val timeoutHandle: Cancellable = delayedMsg(delayBeforeNextWork.fold(work.settings.timeout)(_ + work.settings.timeout), RouteeTimeout)
    delayBeforeNextWork match {
      case Some(d) ⇒
        import context.dispatcher
        context.system.scheduler.scheduleOnce(d, routee, work.messageToDelegatee)
      case None ⇒
        routee ! work.messageToDelegatee
    }
    context become working(Outstanding(work, timeoutHandle, retried))
  }

  private def askMoreWork(delay: Option[FiniteDuration]): Unit = {
    maybeDelayedMsg(delay, RequestWork(self), queue)
    context become waitingForWork
  }

  protected def resultChecker: ResultChecker

  protected def descriptionOf(any: Any, maxLength: Int = 100): String = {
    val msgString = any.toString
    if (msgString.length < maxLength)
      msgString
    else
      msgString.take(msgString.lastIndexWhere(_.isWhitespace, maxLength)).trim + "..."
  }

  protected case class Outstanding(
    work:          Work,
    timeoutHandle: Cancellable,
    retried:       Int           = 0,
    startAt:       LocalDateTime = LocalDateTime.now
  ) {
    def success(result: Any): Unit = {
      monitor ! WorkCompleted(self, startAt.until(LocalDateTime.now))
      done(result)
    }

    def fail(result: Any): Unit = {
      monitor ! WorkFailed(result.toString)
      done(result)
    }

    def timeout(): Unit = {
      monitor ! WorkTimedOut("unknown")
      done(WorkTimedOut(s"Delegatee didn't respond within ${work.settings.timeout}"))
    }

    protected def done(result: Any): Unit = {
      cancel()
      reportResult(result)
    }

    def cancel(): Unit = if (!timeoutHandle.isCancelled) timeoutHandle.cancel()

    lazy val workDescription = descriptionOf(work.messageToDelegatee)

    def reportResult(result: Any): Unit = work.replyTo.foreach(_ ! result)
  }

}

object Worker {

  private case object RouteeTimeout
  private case class RouteeReceived(routee: ActorRef)
  case object Retire

  sealed trait WorkerStatus
  case object Starting extends WorkerStatus
  case object Retiring extends WorkerStatus
  case object Idle extends WorkerStatus
  case object Working extends WorkerStatus

  case class Hold(period: FiniteDuration)

  class DefaultWorker(
    protected val queue:         QueueRef,
    protected val backend:       Backend,
    protected val resultChecker: ResultChecker
  ) extends Worker

  def default(
    queue:   QueueRef,
    backend: Backend
  )(resultChecker: ResultChecker): Props = {
    Props(new DefaultWorker(queue, backend, resultChecker))
  }

}

