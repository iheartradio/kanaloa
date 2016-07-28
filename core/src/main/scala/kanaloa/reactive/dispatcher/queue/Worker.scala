package kanaloa.reactive.dispatcher.queue

import java.time.LocalDateTime

import akka.actor._
import kanaloa.reactive.dispatcher.ApiProtocol.{QueryStatus, WorkFailed, WorkTimedOut}
import kanaloa.reactive.dispatcher.ResultChecker
import kanaloa.reactive.dispatcher.metrics.Metric
import kanaloa.reactive.dispatcher.queue.Queue.{NoWorkLeft, RequestWork, Unregister, Unregistered}
import kanaloa.reactive.dispatcher.queue.WorkSender.{RelayWork, WorkResult}
import kanaloa.reactive.dispatcher.queue.Worker._
import kanaloa.util.Java8TimeExtensions._
import kanaloa.util.MessageScheduler

import scala.concurrent.duration._

private[queue] class Worker(
  queue:                  QueueRef,
  routee:                 ActorRef,
  metricsCollector:       ActorRef,
  circuitBreakerSettings: Option[CircuitBreakerSettings],
  resultChecker:          ResultChecker
) extends Actor with ActorLogging with MessageScheduler {

  //if a WorkSender fails, Let the DeathWatch handle it
  override def supervisorStrategy: SupervisorStrategy = SupervisorStrategy.stoppingStrategy

  var timeoutCount: Int = 0
  var delayBeforeNextWork: Option[FiniteDuration] = None

  var workCounter: Long = 0

  private val circuitBreaker: Option[CircuitBreaker] = circuitBreakerSettings.map(new CircuitBreaker(_))

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

    case Worker.Retire ⇒
      context become waitingToTerminate(outstanding) //when busy no need to unregister, because it's not in the queue's list.
  }

  //This state waits for Work to complete, and then stops the Actor
  def waitingToTerminate(outstanding: Outstanding): Receive = handleRouteeResponse(outstanding, finish) orElse {
    case qs: QueryStatus                           ⇒ qs reply WaitingToTerminate

    //ignore these, since all we care about is the Work completing one way or another
    case Retire | Terminated(`queue`) | NoWorkLeft ⇒

    case w: Work                                   ⇒ sender() ! Rejected(w, "Retiring") //safety first

    case WorkFinished ⇒
      finish() //work is done, terminate
  }

  //in this state, we have told the Queue to Unregister this Worker, so we are waiting for an acknowledgement
  val unregisteringIdle: Receive = {
    case qs: QueryStatus                            ⇒ qs reply UnregisteringIdle

    //ignore these
    case Retire | Terminated(`routee`) | NoWorkLeft ⇒

    case w: Work                                    ⇒ sender ! Rejected(w, "Retiring") //safety first

    //Either we Unregistered successfully, or the Queue died.  terminate
    case Unregistered | Terminated(`queue`) ⇒
      finish()
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

    case Terminated(x) if x == outstanding.workSender ⇒
      log.warning("WorkSender failed!")
      outstanding.fail(WorkFailed(s"due ${routee.path} is terminated"))
      onRouteeFailure()

    case WorkSender.WorkResult(wId, x) if wId == outstanding.workId ⇒ {
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

    case WorkSender.WorkResult(wId, x) ⇒ //should never happen..right?
      log.error("Received a response for a request which has already been serviced")

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
    if (outstanding.retried < outstanding.work.settings.retry) {
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
    workCounter += 1 //do we increase this on a retry?
    val newWorkId = workCounter
    val sender = context.actorOf(WorkSender.props(self, routee, RelayWork(newWorkId, work.messageToDelegatee, delayBeforeNextWork)), "sender-" + newWorkId)
    context.watch(sender)
    val timeout = delayedMsg(delayBeforeNextWork.getOrElse(Duration.Zero) + work.settings.timeout, RouteeTimeout)
    val out = Outstanding(work, newWorkId, timeout, sender, retried)
    context become working(out)
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

  private class CircuitBreaker(settings: CircuitBreakerSettings) {
    def resetTimeoutCount(): Unit = {
      timeoutCount = 0
      delayBeforeNextWork = None
    }

    def incrementTimeoutCount(): Unit = {
      timeoutCount = timeoutCount + 1
      if (timeoutCount >= settings.timeoutCountThreshold) {
        delayBeforeNextWork = Some(settings.openDurationBase * timeoutCount.toLong)
        if (timeoutCount == settings.timeoutCountThreshold) //just crossed the threshold
          metricsCollector ! Metric.CircuitBreakerOpened
      }

    }
  }

  protected case class Outstanding(
    work:          Work,
    workId:        Long,
    timeoutHandle: Cancellable,
    workSender:    ActorRef,
    retried:       Int           = 0,
    startAt:       LocalDateTime = LocalDateTime.now
  ) {
    def success(result: Any): Unit = {
      done(result)
      val duration = startAt.until(LocalDateTime.now)
      circuitBreaker.foreach(_.resetTimeoutCount())
      metricsCollector ! Metric.WorkCompleted(duration)
    }

    def fail(result: Any): Unit = {
      done(result)
      circuitBreaker.foreach(_.resetTimeoutCount())
      metricsCollector ! Metric.WorkFailed
    }

    def timeout(): Unit = {
      done(WorkTimedOut(s"Delegatee didn't respond within ${work.settings.timeout}"))
      circuitBreaker.foreach(_.incrementTimeoutCount())
      metricsCollector ! Metric.WorkTimedOut
    }

    protected def done(result: Any): Unit = {
      cancel()
      reportResult(result)
    }

    def cancel(): Unit = {
      timeoutHandle.cancel()
      context.unwatch(workSender)
      workSender ! PoisonPill
    }
    lazy val workDescription = descriptionOf(work.messageToDelegatee)

    def reportResult(result: Any): Unit = work.replyTo.foreach(_ ! result)

  }
}

private[queue] object Worker {

  private case object RouteeTimeout
  case object Retire

  sealed trait WorkerStatus
  case object UnregisteringIdle extends WorkerStatus
  case object Idle extends WorkerStatus
  case object Working extends WorkerStatus
  case object WaitingToTerminate extends WorkerStatus

  private[queue] case object WorkFinished

  def default(
    queue:                  QueueRef,
    routee:                 ActorRef,
    metricsCollector:       ActorRef,
    circuitBreakerSettings: Option[CircuitBreakerSettings] = None
  )(resultChecker: ResultChecker): Props = {
    Props(new Worker(queue, routee, metricsCollector, circuitBreakerSettings, resultChecker)).withDeploy(Deploy.local)
  }
}

private[queue] class WorkSender(worker: ActorRef, routee: ActorRef, relayWork: RelayWork) extends Actor with MessageScheduler with ActorLogging {

  import relayWork._

  maybeDelayedMsg(delay, message, routee)

  def receive: Receive = {
    case x ⇒ worker ! WorkResult(workId, x)
  }
}

private[queue] object WorkSender {

  case class RelayWork(workId: Long, message: Any, delay: Option[FiniteDuration])

  case class WorkResult(workId: Long, result: Any)

  def props(worker: ActorRef, routee: ActorRef, relayWork: RelayWork): Props = {
    Props(classOf[WorkSender], worker, routee, relayWork)
  }

}
