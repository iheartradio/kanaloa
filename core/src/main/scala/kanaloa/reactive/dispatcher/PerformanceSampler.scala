package kanaloa.reactive.dispatcher

import java.time.{LocalDateTime ⇒ Time}

import akka.actor.{Terminated, ActorRef, Actor}
import kanaloa.reactive.dispatcher.metrics.Metric._
import PerformanceSampler._
import kanaloa.reactive.dispatcher.metrics.{Metric, MetricsCollector}
import kanaloa.util.Java8TimeExtensions._

import scala.concurrent.duration._

/**
 *  Mixed-in with [[MetricsCollector]] to which all [[Metric]] are sent to.
 *  Behind the scene it also collects performance [[Sample]] from [[WorkCompleted]] and [[WorkFailed]]
 *  when the system is in fullyUtilized state, namely when number
 *  of idle workers is less than [[PerformanceSamplerSettings]]
 *  It internally publishes these [[Sample]]s as well as [[PartialUtilization]] data
 *  which are only for internal tuning purpose, and should not be
 *  confused with the [[Metric]] used for realtime monitoring.
 *  It can be subscribed using [[Subscribe]] message.
 *  It publishes [[Sample]]s and [[PartialUtilization]] number to subscribers.
 *
 */
private[dispatcher] trait PerformanceSampler extends Actor {
  self: MetricsCollector ⇒ //todo: it's using cake pattern to mixin with MetricsCollector mainly due to performance reason, there might be ways to achieve more decoupled ways without hurting performance

  val settings: PerformanceSamplerSettings

  var subscribers: Set[ActorRef] = Set.empty

  import settings._

  val scheduledSampling = {
    import context.dispatcher
    context.system.scheduler.schedule(
      sampleRate,
      sampleRate,
      self,
      AddSample
    )
  }

  override def postStop(): Unit = {
    scheduledSampling.cancel()
    super.postStop()
  }

  def receive = partialUtilized(None)

  def handleSubscriptions: Receive = {
    case Subscribe(s) ⇒
      subscribers += s
      context watch s
    case Unsubscribe(s) ⇒
      subscribers -= s
      context unwatch s
    case Terminated(s) ⇒
      subscribers -= s
  }

  def publishUtilization(idle: Int, poolSizeO: Option[Int]): Unit = {
    poolSizeO.foreach { poolSize ⇒
      val utilization = poolSize - idle
      publish(PartialUtilization(utilization))
      report(PoolUtilized(utilization))
    }
  }

  def reportQueueLength(workLeft: Int): Unit =
    report(WorkQueueLength(workLeft))

  def fullyUtilized(s: QueueStatus): Receive = handleSubscriptions orElse {
    case DispatchResult(idle, workLeft) ⇒
      reportQueueLength(workLeft)
      if (!settings.fullyUtilized(idle)) {
        tryComplete(s)
        publishUtilization(idle, s.poolSize)
        context become partialUtilized(s.poolSize)
      }

    case metric: Metric ⇒
      handle(metric) {
        case WorkCompleted(_) | WorkFailed ⇒
          context become fullyUtilized(s.copy(workDone = s.workDone + 1))

        case PoolSize(size) ⇒
          val sizeChanged = s.poolSize.fold(true)(_ != size)
          if (sizeChanged) {
            tryComplete(s)
            context become fullyUtilized(QueueStatus(poolSize = Some(size)))
          }
      }

    case AddSample ⇒
      context become fullyUtilized(tryComplete(s))
      s.poolSize.foreach(s ⇒ report(PoolUtilized(s))) //take the chance to report utilization to reporter

  }

  def partialUtilized(poolSize: Option[Int]): Receive = handleSubscriptions orElse {
    case DispatchResult(idle, workLeft) if settings.fullyUtilized(idle) ⇒
      context become fullyUtilized(QueueStatus(poolSize = poolSize))
      reportQueueLength(workLeft)

    case DispatchResult(idle, _) ⇒
      publishUtilization(idle, poolSize)
    case metric: Metric ⇒
      handle(metric) {
        case PoolSize(s) ⇒
          context become partialUtilized(Some(s))
      }
    case AddSample ⇒
  }

  /**
   *
   * @param status
   * @return a reset status if completes, the original status if not.
   */
  private def tryComplete(status: QueueStatus): QueueStatus = {
    status.toSample(minSampleDuration).fold(status) { sample ⇒
      publish(sample)
      status.copy(workDone = 0, start = Time.now)
    }
  }
  def publish(report: Report): Unit = {
    subscribers.foreach(_ ! report)
  }

}

private[dispatcher] object PerformanceSampler {
  case object AddSample

  case class Subscribe(actorRef: ActorRef)
  case class Unsubscribe(actorRef: ActorRef)

  case class DispatchResult(workersLeft: Int, workLeft: Int)

  /**
   *
   * @param sampleRate
   * @param minSampleDurationRatio minimum sample duration ratio to sample rate. Sample duration less than this will be abandoned.
   * @param fullyUtilized a function that given the number of idle workers indicate if the pool is fully utilized
   */
  case class PerformanceSamplerSettings(
    sampleRate:             FiniteDuration = 1.second,
    minSampleDurationRatio: Double         = 0.3,
    fullyUtilized:          Int ⇒ Boolean  = _ == 0
  ) {
    val minSampleDuration: Duration = sampleRate * minSampleDurationRatio
  }

  case class QueueStatus(
    workDone: Int         = 0,
    start:    Time        = Time.now,
    poolSize: Option[Int] = None
  ) {

    def toSample(minSampleDuration: Duration): Option[Sample] = {
      val end = Time.now
      if (start.until(end) > minSampleDuration
        && workDone > 0
        && poolSize.isDefined) Some(Sample(
        workDone = workDone,
        start = start,
        end = Time.now,
        poolSize = poolSize.get
      ))
      else
        None
    }
  }

  sealed trait Report

  case class Sample(
    workDone: Int,
    start:    Time,
    end:      Time,
    poolSize: Int
  ) extends Report

  /**
   * Number of utilized the workers in the worker when not all workers in the pool are busy
   *
   * @param numOfBusyWorkers
   */
  case class PartialUtilization(numOfBusyWorkers: Int) extends Report
}
