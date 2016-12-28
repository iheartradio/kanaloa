package kanaloa

import java.time.{LocalDateTime ⇒ Time}

import akka.actor.{Props, Actor, ActorRef, Terminated}
import kanaloa.QueueSampler._
import kanaloa.Sampler.{SamplerSettings, Sample, AddSample}
import kanaloa.Types.{QueueLength, Speed}
import kanaloa.metrics.Metric._
import kanaloa.metrics.{Reporter, Metric, MetricsCollector}
import kanaloa.queue.Queue
import kanaloa.queue.Queue.DispatchReport
import kanaloa.util.Java8TimeExtensions._

import scala.concurrent.duration._

/**
 *  Mixed-in with [[MetricsCollector]] to which all [[Metric]] are sent to.
 *  Behind the scene it also collects performance [[QueueSample]] from [[WorkCompleted]] and [[WorkFailed]]
 *  when the system is in fullyUtilized state, namely when number
 *  of idle workers is less than [[kanaloa.Sampler.SamplerSettings]]
 *  It internally publishes these [[QueueSample]]s as well as [[PartialUtilized]] data
 *  which are only for internal tuning purpose, and should not be
 *  confused with the [[Metric]] used for realtime monitoring.
 *  It can be subscribed using [[kanaloa.Sampler.Subscribe]] message.
 *  It publishes [[QueueSample]]s and [[PartialUtilized]] number to subscribers.
 *
 */
private[kanaloa] trait QueueSampler extends Sampler {
  mc: MetricsCollector ⇒ //todo: it's using cake pattern to mixin with MetricsCollector mainly due to performance reason, there might be ways to achieve more decoupled ways without hurting performance

  import settings._

  def receive = partialUtilized

  private def reportQueueLength(queueLength: QueueLength): Unit =
    report(WorkQueueLength(queueLength.value))

  private def fullyUtilized(s: QueueStatus): Receive = {
    def continue(status: Queue.Status, dispatched: Option[Int]): Unit = {
      val Queue.Status(idle, workLeft, isFullyUtilized) = status
      reportQueueLength(workLeft)
      if (!isFullyUtilized) {
        val (rpt, _) = tryComplete(s)
        rpt foreach publish
        context become partialUtilized
        publish(PartialUtilized)
      } else
        context become fullyUtilized(s.copy(queueLength = workLeft, workDone = s.workDone + dispatched.getOrElse(0)))
    }

    handleSubscriptions orElse {
      case r: Queue.DispatchReport ⇒
        continue(r.status, Some(r.dispatched))

      case qs: Queue.Status ⇒
        continue(qs, None)

      case AddSample ⇒
        val (rep, status) = tryComplete(s)
        rep foreach publish
        context become fullyUtilized(status)

      case metric: Metric ⇒
        report(metric)

    }
  }

  private val partialUtilized: Receive = {
    def continue(status: Queue.Status, dispatched: Option[Int]): Unit = {
      val Queue.Status(idle, queueLength, isFullyUtilized) = status
      if (isFullyUtilized) {
        publish(FullyUtilized)
        context become fullyUtilized(
          QueueStatus(queueLength = queueLength, workDone = dispatched.getOrElse(0))
        )
      }
      reportQueueLength(queueLength)
    }

    handleSubscriptions orElse {
      case r: Queue.DispatchReport ⇒
        continue(r.status, Some(r.dispatched))

      case status: Queue.Status ⇒
        continue(status, None)

      case metric: Metric ⇒
        report(metric)

      case AddSample ⇒ //no sample is produced in the partial utilized state
    }
  }

  /**
   *
   * @param status
   * @return a reset status if completes, the original status if not.
   */
  private def tryComplete(status: QueueStatus): (Option[Report], QueueStatus) = {
    val sample = status.toSample(minSampleDuration)

    val newStatus = if (sample.fold(false)(_.dequeued > 0))
      status.copy(workDone = 0, start = Time.now, avgProcessTime = None) //if sample is valid and there is work done restart the counter
    else status

    (sample, newStatus)
  }

}

private[kanaloa] object QueueSampler {

  class QueueSamplerImpl(
    val reporter: Option[Reporter],
    val settings: SamplerSettings
  ) extends MetricsCollector with QueueSampler

  def props(
    reporter: Option[Reporter],
    settings: SamplerSettings  = SamplerSettings()
  ): Props = Props(new QueueSamplerImpl(reporter, settings))

  private case class QueueStatus(
    queueLength:    QueueLength,
    workDone:       Int              = 0,
    start:          Time             = Time.now,
    avgProcessTime: Option[Duration] = None
  ) {

    def toSample(minSampleDuration: Duration): Option[QueueSample] = {
      if (duration >= minSampleDuration) Some(QueueSample(
        dequeued = workDone,
        start = start,
        end = Time.now,
        queueLength = queueLength
      ))
      else
        None
    }

    def duration = start.until(Time.now)

  }

  sealed trait Report extends Sample

  case class QueueSample(
    dequeued:    Int,
    start:       Time,
    end:         Time,
    queueLength: QueueLength
  ) extends Report {
    /**
     * Work dequeued per milliseconds
     */
    lazy val speed: Speed = Speed(dequeued.toDouble * 1000 / start.until(end).toMicros.toDouble)
  }

  case object PartialUtilized extends Report
  case object FullyUtilized extends Report

}
