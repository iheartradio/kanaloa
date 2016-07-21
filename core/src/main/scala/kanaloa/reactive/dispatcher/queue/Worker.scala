package kanaloa.reactive.dispatcher.queue

import java.time.LocalDateTime

import akka.actor._
import kanaloa.reactive.dispatcher.ApiProtocol.{QueryStatus, WorkFailed, WorkTimedOut}
import kanaloa.reactive.dispatcher.metrics.{MetricsCollector, Metric}
import kanaloa.reactive.dispatcher.queue.Queue.{NoWorkLeft, RequestWork, Unregister, Unregistered}
import kanaloa.reactive.dispatcher.queue.QueueProcessor.WorkCompleted
import kanaloa.reactive.dispatcher.queue.Worker._
import kanaloa.reactive.dispatcher.ResultChecker
import kanaloa.util.Java8TimeExtensions._
import kanaloa.util.MessageScheduler

import scala.concurrent.duration._

trait Worker extends Actor with ActorLogging with MessageScheduler {

  val queue: ActorRef
  val metricsCollector: ActorRef
  val resultChecker: ResultChecker
  val circuitBreakerSettings: Option[CircuitBreakerSettings]
  var timeoutCount: Int = 0
  var delayBeforeNextWork: Option[FiniteDuration] = None

  val routee: ActorRef

  def receive = waitingForWork

  override def preStart(): Unit = {
    super.preStart()
    context watch queue
    context watch routee
    queue ! RequestWork(self)
  }

  def finish(): Unit = context stop self

  val waitingForWork: Receive = {
    case qs: QueryStatus                  ⇒ qs reply Idle

    case work: Work                       ⇒ sendWorkToRoutee(work, 0)

    //If there is no work left, or if the Queue dies, the Worker stops as well
    case NoWorkLeft | Terminated(`queue`) ⇒ finish()

    //if the Routee dies or the Worker is told to Retire, it needs to Unregister from the Queue before terminating
    case Terminated(r) if r == routee     ⇒ becomeUnregisteringIdle()
    case Worker.Retire                    ⇒ becomeUnregisteringIdle()
  }

  def working(outstanding: Outstanding): Receive = handleRouteeResponse(outstanding, becomeUnregisteringIdle) orElse {
    case qs: QueryStatus                  ⇒ qs reply Working

    //we are done with this Work, ask for more and wait for it
    case WorkFinished                     ⇒ askMoreWork()
    case w: Work                          ⇒ sender() ! Rejected(w, "Busy")

    //if there is no work left, or if the Queue dies, the Actor must wait for the Work to finish before terminating
    case Terminated(`queue`) | NoWorkLeft ⇒ context become waitingToTerminate(outstanding)

    //This is a fun state.  The Worker is told to stop, but needs to both wait for Unregister and for Work to complete
    case Worker.Retire ⇒
      queue ! Unregister
      context become unregisteringBusy(outstanding)
  }

  //This state waits for Work to complete, and then stops the Actor
  def waitingToTerminate(outstanding: Outstanding): Receive = handleRouteeResponse(outstanding, finish) orElse {
    case qs: QueryStatus                           ⇒ qs reply WaitingToTerminate

    //ignore these, since all we care about is the Work completing one way or another
    case Retire | Terminated(`queue`) | NoWorkLeft ⇒

    case w: Work                                   ⇒ sender() ! Rejected(w, "Retiring") //safety first

    case WorkFinished                              ⇒ finish() //work is done, terminate
  }

  //in this state, we have told the Queue to Unregister this Worker, so we are waiting for an acknowledgement
  def unregisteringIdle: Receive = {
    case qs: QueryStatus                            ⇒ qs reply UnregisteringIdle

    //ignore these
    case Retire | Terminated(`routee`) | NoWorkLeft ⇒

    case w: Work                                    ⇒ sender ! Rejected(w, "Retiring") //safety first

    //Either we Unregistered successfully, or the Queue died.  terminate
    case Unregistered | Terminated(`queue`)         ⇒ finish()

  }

  //in this state we are we waiting for 2 things to happen, Unregistration and Work completing
  //the Worker will shift its state based on which one happens first
  def unregisteringBusy(outstanding: Outstanding): Receive = handleRouteeResponse(outstanding, becomeUnregisteringIdle) orElse {
    case qs: QueryStatus                    ⇒ qs reply UnregisteringBusy

    //ignore these
    case Retire | NoWorkLeft                ⇒

    case w: Work                            ⇒ sender ! Rejected(w, "Retiring") //safety first

    //Either Unregistration completed, or the Queue died, in either way, we just need to wait for Work to finish
    case Unregistered | Terminated(`queue`) ⇒ context become waitingToTerminate(outstanding)

    //work completed on way or another, just waiting for the Unregister ack
    case WorkFinished                       ⇒ context become unregisteringIdle
  }

  def becomeUnregisteringIdle(): Unit = {
    queue ! Unregister(self)
    context become unregisteringIdle
  }

  //onRouteeFailure is what gets called if while waiting for a Routee response, the Routee dies.
  def handleRouteeResponse(outstanding: Outstanding, onRouteeFailure: () ⇒ Unit): Receive = {
    case Terminated(`routee`) ⇒ {
      outstanding.fail(WorkFailed(s"due ${routee.path} is terminated"))
      onRouteeFailure()
    }
    case x if sender() == routee ⇒ {
      val result: Either[String, Any] = resultChecker.applyOrElse(x, failedResultMatch)
      result match {
        case Right(res) ⇒
          outstanding.success(res)
          self ! WorkFinished

        case Left(e) ⇒
          log.warning(s"Error $e returned by routee in regards to running work $outstanding")
          retryOrAbandon(outstanding, e)
      }
    }
    case RouteeTimeout ⇒
      log.warning(s"Routee ${routee.path} timed out after ${outstanding.work.settings.timeout} work ${outstanding.work.messageToDelegatee} abandoned")
      outstanding.timeout()
      self ! WorkFinished

  }

  def failedResultMatch(x: Any): Either[String, Any] = {
    Left(s"Unmatched Result '${descriptionOf(x)}' from the backend service, update your ResultChecker if you want to prevent it from being treated as an error.")
  }

  private def retryOrAbandon(outstanding: Outstanding, error: Any): Unit = {
    outstanding.cancel()
    //why do we fail if there is a delayBeforeNextWork? Is this because of the subsequent 'sendWorkToRoutee' call?
    if (outstanding.retried < outstanding.work.settings.retry && delayBeforeNextWork.isEmpty) {
      log.debug(s"Retry work $outstanding")
      sendWorkToRoutee(outstanding.work, outstanding.retried + 1)
    } else {
      def message = {
        val retryMessage = if (outstanding.retried > 0) s"after ${outstanding.retried + 1} try(s)" else ""
        s"Processing of '${outstanding.workDescription}' failed $retryMessage"
      }
      log.warning(s"$message, work abandoned")
      outstanding.fail(WorkFailed(message + s" due to ${descriptionOf(error)}"))
      self ! WorkFinished
    }
  }

  private def sendWorkToRoutee(work: Work, retried: Int): Unit = {
    val timeoutHandle: Cancellable = {
      maybeDelayedMsg(delayBeforeNextWork, work.messageToDelegatee, routee)
      delayedMsg(delayBeforeNextWork.getOrElse(Duration.Zero) + work.settings.timeout, RouteeTimeout)
    }
    context become working(Outstanding(work, timeoutHandle, retried))
  }

  private def askMoreWork(): Unit = {
    maybeDelayedMsg(delayBeforeNextWork, RequestWork(self), queue)
    context become waitingForWork
  }

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
      done(result)
      val duration = startAt.until(LocalDateTime.now)
      resetTimeoutCount()
      metricsCollector ! Metric.WorkCompleted(duration)
    }

    def fail(result: Any): Unit = {
      done(result)
      resetTimeoutCount()
      metricsCollector ! Metric.WorkFailed
    }

    def timeout(): Unit = {
      done(WorkTimedOut(s"Delegatee didn't respond within ${work.settings.timeout}"))
      incrementTimeoutCount()
      metricsCollector ! Metric.WorkTimedOut
    }

    protected def done(result: Any): Unit = {
      cancel()
      reportResult(result)
    }

    def cancel(): Unit = if (!timeoutHandle.isCancelled) timeoutHandle.cancel()

    lazy val workDescription = descriptionOf(work.messageToDelegatee)

    def reportResult(result: Any): Unit = work.replyTo.foreach(_ ! result)

    def resetTimeoutCount(): Unit = {
      timeoutCount = 0
      delayBeforeNextWork = None
    }

    def incrementTimeoutCount(): Unit = {
      timeoutCount = timeoutCount + 1
      circuitBreakerSettings.foreach { cbs ⇒
        if (timeoutCount >= cbs.timeoutCountThreshold) {
          delayBeforeNextWork = Some(cbs.openDurationBase * timeoutCount)
          if (timeoutCount == cbs.timeoutCountThreshold) //just crossed the threshold
            metricsCollector ! Metric.CircuitBreakerOpened
        }
      }
    }
  }

}

object Worker {

  private case object RouteeTimeout
  case object Retire

  sealed trait WorkerStatus
  case object UnregisteringIdle extends WorkerStatus
  case object UnregisteringBusy extends WorkerStatus
  case object Idle extends WorkerStatus
  case object Working extends WorkerStatus
  case object WaitingToTerminate extends WorkerStatus

  case class SetDelay(period: Option[FiniteDuration])
  private[queue] case object WorkFinished

  class DefaultWorker(
    val queue:                  QueueRef,
    val routee:                 ActorRef,
    val metricsCollector:       ActorRef,
    val circuitBreakerSettings: Option[CircuitBreakerSettings],
    val resultChecker:          ResultChecker
  ) extends Worker

  def default(
    queue:                  QueueRef,
    routee:                 ActorRef,
    metricsCollector:       ActorRef,
    circuitBreakerSettings: Option[CircuitBreakerSettings] = None
  )(resultChecker: ResultChecker): Props = {
    Props(new DefaultWorker(queue, routee, metricsCollector, circuitBreakerSettings, resultChecker)).withDeploy(Deploy.local)
  }
}
