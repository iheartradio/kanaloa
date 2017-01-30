package kanaloa.queue

import java.time.{LocalDateTime ⇒ Time}

import akka.actor.{ActorRef, Props}
import kanaloa.Types.Speed
import kanaloa.metrics.Metric._
import kanaloa.metrics.{Metric, Reporter, WorkerPoolMetricsCollector}
import kanaloa.queue.QueueSampler.{PartialUtilized, FullyUtilized}
import kanaloa.queue.WorkerPoolSampler._
import kanaloa.util.Java8TimeExtensions._
import Sampler._
import scala.concurrent.duration._

/**
 *  Mixed-in with [[WorkerPoolMetricsCollector]] to which all [[Metric]] are sent to.
 *  Behind the scene it also collects performance [[kanaloa.queue.WorkerPoolSampler.WorkerPoolSample]] from [[WorkCompleted]] and [[WorkFailed]]
 *  when the system is in fullyUtilized state, namely when number
 *  of idle workers is less than [[kanaloa.queue.Sampler.SamplerSettings]]
 */
private[kanaloa] trait WorkerPoolSampler extends Sampler {
  mc: WorkerPoolMetricsCollector ⇒

  def queueSampler: ActorRef

  override def preStart(): Unit = {
    queueSampler ! Subscribe(self)
  }

  override def postStop(): Unit = {
    queueSampler ! Unsubscribe(self)
    super.postStop()
  }

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
    val sample = status.toSample(settings.minSampleDuration)

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
    workDone:            Int              = 0,
    workingWorkers:      Int              = 0,
    start:               Time             = Time.now,
    poolSize:            Int              = 0,
    avgProcessTime:      Option[Duration] = None,
    consecutiveTimeouts: Int              = 0
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
  case class PartialUtilization(numOfBusyWorkers: Int) extends Report

  class WorkerPoolSamplerImpl(
    val reporter:         Option[Reporter],
    val queueSampler:     ActorRef,
    val settings:         SamplerSettings,
    val metricsForwardTo: Option[ActorRef]
  ) extends WorkerPoolMetricsCollector with WorkerPoolSampler

  def props(
    reporter:         Option[Reporter],
    queueSampler:     ActorRef,
    settings:         SamplerSettings,
    metricsForwardTo: Option[ActorRef]

  ): Props = Props(new WorkerPoolSamplerImpl(reporter, queueSampler, settings, metricsForwardTo))
}
