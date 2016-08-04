package kanaloa.reactive.dispatcher

import java.time.{LocalDateTime ⇒ Time}

import akka.actor.{Terminated, ActorRef, Actor}
import kanaloa.reactive.dispatcher.ApiProtocol.QueryStatus
import kanaloa.reactive.dispatcher.Types.{Speed, QueueLength}
import kanaloa.reactive.dispatcher.metrics.Metric._
import PerformanceSampler._
import kanaloa.reactive.dispatcher.metrics.{Metric, MetricsCollector}
import kanaloa.reactive.dispatcher.queue.Queue
import kanaloa.reactive.dispatcher.queue.Queue.Status
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
      sampleInterval,
      sampleInterval,
      self,
      AddSample
    )
  }

  override def postStop(): Unit = {
    scheduledSampling.cancel()
    super.postStop()
  }

  def receive = partialUtilized(0)

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

  def publishUtilization(idle: Int, poolSize: Int): Unit = {
    val utilization = poolSize - idle
    publish(PartialUtilization(utilization))
    report(PoolUtilized(utilization))
  }

  def reportQueueLength(queueLength: QueueLength): Unit =
    report(WorkQueueLength(queueLength.value))

  def fullyUtilized(s: QueueStatus): Receive = handleSubscriptions orElse {
    case Queue.Status(idle, workLeft, isFullyUtilized) ⇒
      reportQueueLength(workLeft)
      if (!isFullyUtilized) {
        val (rpt, _) = tryComplete(s)
        rpt foreach publish
        publishUtilization(idle, s.poolSize)
        context become partialUtilized(s.poolSize)
      } else
        context become fullyUtilized(s.copy(queueLength = workLeft))

    case metric: Metric ⇒
      handle(metric) {
        case WorkCompleted(processTime) ⇒
          val newWorkDone = s.workDone + 1
          val newAvgProcessTime = s.avgProcessTime.fold(processTime)(avg ⇒ ((avg * s.workDone.toDouble + processTime) / newWorkDone.toDouble))
          context become fullyUtilized(
            s.copy(
              workDone = newWorkDone,
              avgProcessTime = Some(newAvgProcessTime)
            )
          )
        case WorkFailed ⇒
          context become fullyUtilized(s.copy(workDone = s.workDone + 1))

        case PoolSize(size) ⇒
          val sizeChanged = s.poolSize != size
          if (sizeChanged) {
            val (r, _) = tryComplete(s)
            r foreach publish
            context become fullyUtilized(
              QueueStatus(poolSize = size, queueLength = s.queueLength)
            )
          }
      }

    case AddSample ⇒
      val (rep, status) = tryComplete(s)
      rep foreach publish
      context become fullyUtilized(status)
      report(PoolUtilized(s.poolSize)) //take the chance to report utilization to reporter
  }

  def partialUtilized(poolSize: Int): Receive = handleSubscriptions orElse {
    case Queue.Status(idle, queueLength, isFullyUtilized) ⇒
      if (isFullyUtilized) {
        context become fullyUtilized(
          QueueStatus(poolSize = poolSize, queueLength = queueLength)
        )
      } else
        publishUtilization(idle, poolSize)
      reportQueueLength(queueLength)

    case metric: Metric ⇒
      handle(metric) {
        case PoolSize(s) ⇒
          context become partialUtilized(s)
      }
    case AddSample ⇒ //no sample is produced in the partial utilized state
  }

  /**
   *
   * @param status
   * @return a reset status if completes, the original status if not.
   */
  private def tryComplete(status: QueueStatus): (Option[Report], QueueStatus) = {
    val sample = status.toSample(minSampleDuration)

    val newStatus = if (sample.fold(false)(_.workDone > 0))
      status.copy(workDone = 0, start = Time.now, avgProcessTime = None) //if sample is valid and there is work done restart the counter
    else status

    (sample, newStatus)
  }

  def publish(report: Report): Unit = {
    subscribers.foreach(_ ! report)
  }

}

private[dispatcher] object PerformanceSampler {
  case object AddSample

  case class Subscribe(actorRef: ActorRef)
  case class Unsubscribe(actorRef: ActorRef)

  /**
   *
   * @param sampleInterval do one sampling each interval
   * @param minSampleDurationRatio minimum sample duration ratio to [[sampleInterval]]. Sample whose duration is less than this will be abandoned.
   */
  case class PerformanceSamplerSettings(
    sampleInterval:         FiniteDuration = 1.second,
    minSampleDurationRatio: Double         = 0.3
  ) {
    val minSampleDuration: Duration = sampleInterval * minSampleDurationRatio
  }

  case class QueueStatus(
    queueLength:    QueueLength,
    workDone:       Int              = 0,
    start:          Time             = Time.now,
    poolSize:       Int              = 0,
    avgProcessTime: Option[Duration] = None
  ) {

    def toSample(minSampleDuration: Duration): Option[Sample] = {
      if (duration >= minSampleDuration) Some(Sample(
        workDone = workDone,
        start = start,
        end = Time.now,
        poolSize = poolSize,
        queueLength = queueLength,
        avgProcessTime = avgProcessTime
      ))
      else
        None
    }

    def duration = start.until(Time.now)

  }

  sealed trait Report

  case class Sample(
    workDone:       Int,
    start:          Time,
    end:            Time,
    poolSize:       Int,
    queueLength:    QueueLength,
    avgProcessTime: Option[Duration]
  ) extends Report {
    /**
     * Work done per milliseconds
     */
    lazy val speed: Speed = Speed(workDone.toDouble * 1000 / start.until(end).toMicros.toDouble)
  }

  /**
   * Number of utilized the workers in the worker when not all workers in the pool are busy
   *
   * @param numOfBusyWorkers
   */
  case class PartialUtilization(numOfBusyWorkers: Int) extends Report

}
