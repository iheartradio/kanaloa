package com.iheart.workpipeline.akka.patterns.queue

import akka.actor._
import com.iheart.workpipeline.akka.helpers.MessageScheduler
import com.iheart.workpipeline.akka.patterns
import com.iheart.workpipeline.collection.FiniteCollection
import com.iheart.workpipeline.metrics.{MetricsCollector, NoOpMetricsCollector, Metric}
import patterns.CommonProtocol.QueryStatus
import CommonProtocol.{WorkTimedOut, WorkFailed}
import QueueProcessor.MissionAccomplished
import Queue.{Unregistered, Unregister, NoWorkLeft, RequestWork}
import Worker._

import scala.concurrent.duration._
import FiniteCollection._

trait Worker extends Actor with ActorLogging with MessageScheduler {
  type ResultHistory = Vector[Boolean]

  protected def delegateeProps: Props //actor who really does the work
  protected val queue: ActorRef
  protected def monitor: ActorRef = context.parent
  protected val metricsCollector: MetricsCollector

  def receive = idle()

  def resultHistoryLength: Int

  //hate to have a var here, but this field avoid having to pass this history all over the places.
  protected var resultHistory: ResultHistory = Vector.empty

  context watch queue

  override def preStart(): Unit = {
    askMoreWork()
  }

  lazy val delegatee = {
    val ref = context.actorOf(delegateeProps, "delegatee")
    context watch ref
    ref
  }

  def idle(): Receive = {
    case work : Work => sendWorkToDelegatee(work, 0)

    case NoWorkLeft =>
      monitor ! MissionAccomplished(self) //todo: maybe a simple stop is good enough?
      finish()

    case Worker.Retire =>
      queue ! Unregister(self)
      context become retiring(None)

    case qs: QueryStatus => qs reply Idle

    case Terminated(`queue`) => finish()
  }

  def finish(): Unit = context stop self

  def working(outstanding: Outstanding): Receive = ({
      case Terminated(`queue`) => context become retiring(Some(outstanding))

      case qs: QueryStatus => qs reply Working

      case Worker.Retire => context become retiring(Some(outstanding))

  }: Receive).orElse(

    waitingResult(outstanding, false))

  .orElse {
      case msg => log.error(s"unrecognized interrupting msg during working $msg" )
    }

  def retiring(outstanding: Option[Outstanding]): Receive =  ({
    case Terminated(`queue`) => //ignore when retiring
    case qs: QueryStatus => qs reply Retiring
    case Unregistered => finish()
    case Retire => //already retiring
  }: Receive) orElse (
    if(outstanding.isDefined)
      waitingResult(outstanding.get, true)
    else {
      case w: Work =>
        sender ! Rejected(w, "Retiring")
        finish()
    }
  )


  def waitingResult(outstanding: Outstanding, isRetiring: Boolean): Receive = ({

    case DelegateeTimeout =>
      log.error(s"${delegatee.path} timed out after ${outstanding.work.settings.timeout} work ${outstanding.work.messageToDelegatee} abandoned")
      outstanding.timeout()

      if(isRetiring) finish() else {
        appendResultHistory(false)
        askMoreWork()
      }
    case w: Work => sender ! Rejected(w, "busy") //just in case

  }: Receive) orElse resultChecker.andThen[Unit] {
      case Right(result) =>
        outstanding.success(result)
        if(isRetiring) finish() else {
          appendResultHistory(true)
          askMoreWork()
        }
      case Left(e) =>
        log.error(s"error $e returned by delegatee in regards to running work $outstanding")
        appendResultHistory(false)
        retryOrAbandon(outstanding, isRetiring, e)

    }

  private def retryOrAbandon(outstanding: Outstanding, isRetiring: Boolean, error: Any): Unit = {
    outstanding.cancel()
    if (outstanding.retried < outstanding.work.settings.retry ) {
      log.info(s"Retry work $outstanding")
      sendWorkToDelegatee(outstanding.work, outstanding.retried + 1)
    } else {
      val message = s"Work failed after ${outstanding.retried} try(s)"
      log.error(s"$message, work $outstanding abandoned")
      outstanding.fail(WorkFailed(message + s" due to $error"))
      if(isRetiring) finish()
      else
        askMoreWork()
    }
  }

  private def sendWorkToDelegatee(work: Work, retried: Int): Unit = {
    val timeoutHandle: Cancellable = delayedMsg(work.settings.timeout, DelegateeTimeout)
    delegatee ! work.messageToDelegatee
    context become working(Outstanding(work, timeoutHandle, retried))
  }

  private def askMoreWork(): Unit = {
    val delay = holdOnGettingMoreWork
    if(delay.isDefined)
      delayedMsg(delay.get, RequestWork(self), queue)
    else
      queue ! RequestWork(self)
    context become idle
  }

  private def appendResultHistory(result: Boolean): Unit =
    resultHistory = resultHistory.enqueueFinite(result, resultHistoryLength)


  protected def resultChecker: ResultChecker

  protected def holdOnGettingMoreWork: Option[FiniteDuration]

  protected case class Outstanding(work: Work, timeoutHandle: Cancellable, retried: Int = 0) {
    def success(result: Any): Unit = {
      metricsCollector.send(Metric.WorkCompleted)
      done(result)
    }

    def fail(result: Any): Unit = {
      metricsCollector.send(Metric.WorkFailed)
      done(result)
    }

    def timeout(): Unit = {
      metricsCollector.send(Metric.WorkTimedOut)
      done(WorkTimedOut(s"Delegatee didn't respond within ${work.settings.timeout}"))
    }

    protected def done(result: Any): Unit = {
      cancel()
      reportResult(result)
    }

    def cancel(): Unit = if(!timeoutHandle.isCancelled) timeoutHandle.cancel()


    override def toString = work.messageToDelegatee.getClass.toString

    def reportResult(result: Any): Unit = work.settings.sendResultTo.foreach(_ ! result)

  }
}


object Worker {

  private case object DelegateeTimeout
  case object Retire

  sealed trait WorkerStatus
  case object Retiring extends WorkerStatus
  case object Idle extends WorkerStatus
  case object Working extends WorkerStatus


  class DefaultWorker(protected val queue: QueueRef,
                      protected val delegateeProps: Props,
                      protected val resultChecker: ResultChecker,
                      protected val metricsCollector: MetricsCollector = NoOpMetricsCollector) extends Worker {

    val resultHistoryLength = 0
    protected def holdOnGettingMoreWork: Option[FiniteDuration] = None

  }

  class WorkerWithCircuitBreaker( protected val queue: QueueRef,
                                  protected val delegateeProps: Props,
                                  protected val resultChecker: ResultChecker,
                                  circuitBreakerSettings: CircuitBreakerSettings,
                                  protected val metricsCollector: MetricsCollector = NoOpMetricsCollector) extends Worker {

    protected def holdOnGettingMoreWork: Option[FiniteDuration] = {
      if( (resultHistory.count(r => !r).toDouble / resultHistoryLength) >= circuitBreakerSettings.errorRateThreshold  )
        Some(circuitBreakerSettings.closeDuration)
      else
        None
    }
    val resultHistoryLength = circuitBreakerSettings.historyLength
  }



  def default(queue: QueueRef,
              delegateeProps: Props,
              metricsCollector: MetricsCollector = NoOpMetricsCollector)(resultChecker: ResultChecker): Props = {
    Props(new DefaultWorker(queue, delegateeProps, resultChecker, metricsCollector))
  }

  def withCircuitBreaker(queue: QueueRef,
                         delegateeProps: Props,
                         circuitBreakerSettings: CircuitBreakerSettings,
                         metricsCollector: MetricsCollector = NoOpMetricsCollector)(resultChecker: ResultChecker): Props = {
    Props(new WorkerWithCircuitBreaker(queue, delegateeProps, resultChecker, circuitBreakerSettings, metricsCollector))
  }

}

