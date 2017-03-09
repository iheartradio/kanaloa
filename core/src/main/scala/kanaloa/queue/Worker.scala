package kanaloa.queue

import java.time.LocalDateTime

import akka.actor._
import kanaloa.ApiProtocol.{QueryStatus, WorkFailed, WorkTimedOut}
import kanaloa.handler._

import kanaloa.metrics.Metric
import kanaloa.queue.Queue.{NoWorkLeft, RequestWork, Unregister, Unregistered}
import kanaloa.queue.Worker._
import kanaloa.queue.WorkerPoolSampler.{WorkerStoppedWorking, WorkerStartWorking}
import kanaloa.util.Java8TimeExtensions._
import kanaloa.util.MessageScheduler, kanaloa.util.AnyEq._

import scala.concurrent.duration._

private[queue] class Worker[T](
  queue:            QueueRef,
  metricsCollector: ActorRef,
  handler:          Handler[T]

) extends Actor with ActorLogging with MessageScheduler {

  //if a WorkSender fails, Let the DeathWatch handle it
  override def supervisorStrategy: SupervisorStrategy = SupervisorStrategy.stoppingStrategy

  var timeoutCount: Int = 0
  var delayBeforeNextWork: Option[FiniteDuration] = None

  var workCounter: Long = 0

  def receive = waitingForWork

  override def preStart(): Unit = {
    super.preStart()
    queue ! RequestWork(self)
  }

  def finish(reason: String): Unit = {
    log.debug(s"Stopping due to: $reason")
    context stop self
  }

  val handleHoldMessage: Receive = {
    case DelayBeforeNextWork(value) ⇒
      delayBeforeNextWork = Some(value)
    case CancelDelay ⇒
      delayBeforeNextWork = None
  }

  val waitingForWork: Receive = handleHoldMessage orElse {
    case qs: QueryStatus ⇒ qs reply Idle

    case work: Work[T] ⇒
      if (delayBeforeNextWork.isDefined) {
        queue ! Rejected(work, "onHold")
        askMoreWork()
      } else
        sendWorkToHandler(work, 0)

    //If there is no work left, or if the Queue dies, the Worker stops as well
    case NoWorkLeft ⇒ finish("Queue reports no Work left")

    //if the Routee dies or the Worker is told to Retire, it needs to Unregister from the Queue before terminating
    case Worker.Retire ⇒
      becomeUnregisteringIdle()
  }

  def working(outstanding: Outstanding): Receive = handleHoldMessage orElse handleRouteeResponse(outstanding)(askMoreWork()) orElse {
    case qs: QueryStatus ⇒ qs reply Working

    case w: Work[_]      ⇒ queue ! Rejected(w, "Busy")

    //if there is no work left, or if the Queue dies, the Actor must wait for the Work to finish before terminating
    case NoWorkLeft      ⇒ context become waitingToTerminate(outstanding)

    case Worker.Retire ⇒
      context become waitingToTerminate(outstanding) //when busy no need to unregister, because it's not in the queue's list.
  }

  //This state waits for Work to complete, and then stops the Actor
  def waitingToTerminate(outstanding: Outstanding): Receive = handleRouteeResponse(outstanding)(finish(s"Finished work, time to retire")) orElse {
    case qs: QueryStatus     ⇒ qs reply WaitingToTerminate

    //ignore these, since all we care about is the Work completing one way or another
    case Retire | NoWorkLeft ⇒

    case w: Work[_]          ⇒ sender() ! Rejected(w, "Retiring") //safety first

  }

  //in this state, we have told the Queue to Unregister this Worker, so we are waiting for an acknowledgement
  val unregisteringIdle: Receive = {
    case qs: QueryStatus     ⇒ qs reply UnregisteringIdle

    //ignore these
    case Retire | NoWorkLeft ⇒

    case w: Work[T]          ⇒ sender ! Rejected(w, "Retiring") //safety first

    //Either we Unregistered successfully, or the Queue died.  terminate
    case Unregistered ⇒
      finish(s"Idle worker finished unregistration.")

    case Terminated(`queue`) ⇒
      finish("Idle working detected Queue terminated")
  }

  def becomeUnregisteringIdle(): Unit = {
    queue ! Unregister(self)
    context watch queue //queue might die first
    context become unregisteringIdle
  }

  //onRouteeFailure is what gets called if while waiting for a Routee response, the Routee dies.
  def handleRouteeResponse(outstanding: Outstanding)(onComplete: ⇒ Unit): Receive = {

    case wr: WorkResult[handler.Resp, handler.Error] @unchecked if wr.workId === outstanding.workId ⇒ {
      wr.result.instruction.foreach(context.parent ! _) //forward instruction back to parent the WorkerPoolManager

      wr.result.reply match {
        case Right(res) ⇒
          outstanding.success(res)
          onComplete

        case Left(e) ⇒
          abandon(outstanding, e)(onComplete)
      }

    }

    case WorkResult(wId, x) ⇒
      log.warning("Received a response for a request which has already been serviced/timedout")

    case HandlerTimeout ⇒
      log.warning(s"handler ${handler.name} timed out after ${outstanding.work.settings.serviceTimeout} work ${outstanding.work.messageToDelegatee} abandoned")
      outstanding.timeout()
      onComplete
  }

  private def abandon(outstanding: Outstanding, error: handler.Error)(onComplete: ⇒ Unit): Unit = {

    val errorDesc = descriptionOf(error, outstanding.work.settings.lengthOfDisplayForMessage).map { e ⇒
      s"due to $e"
    }.getOrElse("")

    def message = {
      val retryMessage = if (outstanding.retried > 0) s"after ${outstanding.retried + 1} try(s)" else ""
      s"Processing of '${outstanding.workDescription}' failed $retryMessage $errorDesc"
    }

    log.warning(s"$message, work abandoned")
    outstanding.fail(WorkFailed(message))
    onComplete

  }

  private def sendWorkToHandler(work: Work[T], retried: Int): Unit = {
    workCounter += 1 //do we increase this on a retry?
    val newWorkId = workCounter

    val handling = handler.handle(work.messageToDelegatee)
    import context.dispatcher
    handling.result.foreach { r ⇒
      self ! WorkResult(newWorkId, r)
    }
    val timeout = delayedMsg(work.settings.serviceTimeout, HandlerTimeout)
    val out = Outstanding(work, newWorkId, timeout, handling, retried)
    context become working(out)
    metricsCollector ! WorkerStartWorking

  }

  private def askMoreWork(): Unit = {
    maybeDelayedMsg(delayBeforeNextWork, RequestWork(self), queue)
    delayBeforeNextWork = None
    context become waitingForWork
  }

  protected case class Outstanding(
    work:          Work[T],
    workId:        Long,
    timeoutHandle: akka.actor.Cancellable,
    handling:      Handling[_, _],
    retried:       Int                    = 0,
    startAt:       LocalDateTime          = LocalDateTime.now
  ) {
    def success(result: Any): Unit = {
      done(result)
      val duration = startAt.until(LocalDateTime.now)
      metricsCollector ! Metric.WorkCompleted(duration)
    }

    def fail(result: Any): Unit = {
      done(result)
      metricsCollector ! Metric.WorkFailed
    }

    def retry(): Unit = {
      queue ! Rejected(work, "No response from service within timeout, retry.")
      cancel()
    }

    def timeout(): Unit = {
      if (work.settings.atLeastOnce && !work.expired)
        retry()
      else
        done(WorkTimedOut(s"Delegatee didn't respond within ${work.settings.serviceTimeout}"))

      metricsCollector ! Metric.WorkTimedOut
    }

    protected def done(result: Any): Unit = {
      cancel()
      reportResult(result)
    }

    def cancel(): Unit = {
      metricsCollector ! WorkerStoppedWorking
      timeoutHandle.cancel()
      handling.cancellable.foreach(_.cancel())
    }

    lazy val workDescription = descriptionOf(work.messageToDelegatee, work.settings.lengthOfDisplayForMessage).get

    def reportResult(result: Any): Unit = work.replyTo.foreach(_ ! result)

  }
}

private[queue] object Worker {
  case class WorkResult[TResp, TError](workId: Long, result: Result[TResp, TError])

  private case object HandlerTimeout
  case class DelayBeforeNextWork(value: FiniteDuration)
  case object CancelDelay
  case object Retire

  sealed trait WorkerStatus
  case object UnregisteringIdle extends WorkerStatus
  case object Idle extends WorkerStatus
  case object Working extends WorkerStatus

  case object WaitingToTerminate extends WorkerStatus

  def default[T](
    queue:                  QueueRef,
    handler:                Handler[T],
    metricsCollector:       ActorRef,
    circuitBreakerSettings: Option[CircuitBreakerSettings] = None
  ): Props = {
    Props(new Worker[T](queue, metricsCollector, handler)).withDeploy(Deploy.local)
  }

  private[queue] def descriptionOf(any: Any, maxLength: Int): Option[String] = {
    any match {
      case None ⇒ None
      case other ⇒ Some {
        val msgString = other.toString
        if (msgString.length < maxLength)
          msgString
        else {
          val firstWhitespaceAfterMax = msgString.indexWhere(c ⇒ (c.isWhitespace || !c.isLetterOrDigit), maxLength)
          val truncateAt = if (firstWhitespaceAfterMax < 0) maxLength else firstWhitespaceAfterMax
          msgString.take(truncateAt).trim + "..."
        }
      }
    }

  }
}
