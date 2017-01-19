package kanaloa

import java.time.{LocalDateTime ⇒ Time}

import akka.actor.{Props, Actor, ActorRef, Terminated}
import kanaloa.QueueSampler.{PartialUtilized, FullyUtilized}
import kanaloa.WorkerPoolSampler._
import kanaloa.Sampler._
import kanaloa.Types.{QueueLength, Speed}
import kanaloa.metrics.Metric._
import kanaloa.metrics.{WorkerPoolMetricsCollector, Reporter, Metric}
import kanaloa.queue.Queue
import kanaloa.util.Java8TimeExtensions._

import scala.concurrent.duration._

/**
 *  Mixed-in with [[WorkerPoolMetricsCollector]] to which all [[Metric]] are sent to.
 *  Behind the scene it also collects performance [[WorkerPoolSample]] from [[WorkCompleted]] and [[WorkFailed]]
 *  when the system is in fullyUtilized state, namely when number
 *  of idle workers is less than [[kanaloa.Sampler.SamplerSettings]]
 *  It internally publishes these [[WorkerPoolSample]]s as well as [[PartialUtilization]] data
 *  which are only for internal tuning purpose, and should not be
 *  confused with the [[Metric]] used for realtime monitoring.
 *  It can be subscribed using [[kanaloa.Sampler.Subscribe]] message.
 *  It publishes [[WorkerPoolSample]]s and [[PartialUtilization]] number to subscribers.
 *
 */
private[kanaloa] trait WorkerPoolSampler extends Sampler {
  mc: WorkerPoolMetricsCollector ⇒ //todo: it's using cake pattern to mixin with WorkerPoolMetricsCollector mainly due to performance reason, there might be ways to achieve more decoupled ways without hurting performance

  def queueSampler: ActorRef

  override def preStart(): Unit = {
    queueSampler ! Subscribe(self)
  }

  override def postStop(): Unit = {
    queueSampler ! Unsubscribe(self)
    super.postStop()
  }

  import settings._

  def receive = partialUtilized(PartialUtilizedPoolStatus())

  private def fullyUtilized(s: PoolStatus): Receive = handleSubscriptions orElse {

    case FullyUtilized ⇒ //ignore
    case PartialUtilized ⇒
      val (rpt, _) = tryComplete(s)
      rpt foreach publish
      report(PoolUtilized(s.workingWorkers))
      context become partialUtilized(
        PartialUtilizedPoolStatus(
          workingWorkers = s.workingWorkers, poolSize = s.poolSize
        )
      )

    case metric: WorkerPoolMetric ⇒
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
              PoolStatus(poolSize = size)
            )
          }
      }

    case AddSample ⇒
      val (rep, status) = tryComplete(s)
      rep foreach publish
      context become fullyUtilized(status)
      report(PoolUtilized(s.poolSize)) //take the chance to report utilization to reporter
  }

  private def partialUtilized(status: PartialUtilizedPoolStatus): Receive = handleSubscriptions orElse {
    case PartialUtilized ⇒
      report(PoolUtilized(status.workingWorkers))
    case FullyUtilized ⇒
      context become fullyUtilized(
        PoolStatus(poolSize = status.poolSize, workingWorkers = status.workingWorkers)
      )

    case metric: WorkerPoolMetric ⇒
      handle(metric) {
        case PoolSize(s) ⇒
          context become partialUtilized(status.copy(poolSize = s))
      }
    case AddSample ⇒ //no sample is produced in the partial utilized state
  }

  /**
   * @return a reset status if completes, the original status if not.
   */
  private def tryComplete(status: PoolStatus): (Option[Report], PoolStatus) = {
    val sample = status.toSample(minSampleDuration)

    val newStatus = if (sample.fold(false)(_.workDone > 0))
      status.copy(workDone = 0, start = Time.now, avgProcessTime = None) //if sample is valid and there is work done restart the counter
    else status

    (sample, newStatus)
  }

}

private[kanaloa] object WorkerPoolSampler {

  private case class PartialUtilizedPoolStatus(
    workingWorkers: Int = 0,
    poolSize:       Int = 0
  )
  private case class PoolStatus(
    workDone:       Int              = 0,
    workingWorkers: Int              = 0,
    start:          Time             = Time.now,
    poolSize:       Int              = 0,
    avgProcessTime: Option[Duration] = None
  ) {

    def toSample(minSampleDuration: Duration): Option[WorkerPoolSample] = {
      if (duration >= minSampleDuration) Some(WorkerPoolSample(
        workDone = workDone,
        start = start,
        end = Time.now,
        poolSize = poolSize,
        avgProcessTime = avgProcessTime
      ))
      else
        None
    }

    def duration = start.until(Time.now)

  }

  sealed trait Report extends Sample

  case class WorkerPoolSample(
    workDone:       Int,
    start:          Time,
    end:            Time,
    poolSize:       Int,
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
  //todo: move this to queue sampler
  case class PartialUtilization(numOfBusyWorkers: Int) extends Report

  class MetricsCollectorImpl(
    val reporter:     Option[Reporter],
    val queueSampler: ActorRef,
    val settings:     SamplerSettings
  ) extends WorkerPoolMetricsCollector with WorkerPoolSampler

  def props(
    reporter:     Option[Reporter],
    queueSampler: ActorRef,
    settings:     SamplerSettings  = SamplerSettings()
  ): Props = Props(new MetricsCollectorImpl(reporter, queueSampler, settings))
}
