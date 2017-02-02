package kanaloa.queue

import java.time.{LocalDateTime ⇒ Time}

import akka.actor.{Props, Actor, ActorRef, Terminated}
import kanaloa.queue.QueueSampler._
import kanaloa.queue.Sampler.{SamplerSettings, Sample, AddSample}
import kanaloa.Types.{QueueLength, Speed}
import kanaloa.metrics.Metric._
import kanaloa.metrics.{Reporter, Metric}
import kanaloa.util.Java8TimeExtensions._

import scala.concurrent.duration._

/**
 *  It can be subscribed using [[kanaloa.queue.Sampler.Subscribe]] message.
 *  It publishes [[QueueSample]]s and [[PartialUtilized]] number to subscribers.
 *
 */
private[kanaloa] trait QueueSampler extends Sampler {

  import settings._

  def reporter: Option[Reporter]

  def receive = partialUtilized

  private def reportQueueLength(queueLength: QueueLength): Unit =
    report(WorkQueueLength(queueLength.value))

  private def overflown(s: QueueStatus): Receive = {
    def continue(status: Queue.Status, dispatched: Option[Int]): Unit = {
      val Queue.Status(idle, workLeft, isOverflown) = status
      reportQueueLength(workLeft)
      if (!isOverflown) {
        val (rpt, _) = tryComplete(s)
        rpt foreach publish
        context become partialUtilized
        publish(PartialUtilized)
      } else
        context become overflown(s.copy(queueLength = workLeft, workDone = s.workDone + dispatched.getOrElse(0)))
    }

    handleSubscriptions orElse {
      case r: Queue.DispatchReport ⇒
        continue(r.status, Some(r.dispatched))

      case qs: Queue.Status ⇒
        continue(qs, None)

      case AddSample ⇒
        val (rep, status) = tryComplete(s)
        rep foreach publish
        context become overflown(status)

      case metric: QueueMetric ⇒
        report(metric)
    }
  }

  private val partialUtilized: Receive = {
    def continue(status: Queue.Status, dispatched: Option[Int]): Unit = {
      val Queue.Status(idle, queueLength, isOverflown) = status
      if (isOverflown) {
        publish(Overflown)
        context become overflown(
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

      case metric: QueueMetric ⇒
        report(metric)

      case AddSample ⇒ //no sample is produced in the partial utilized state
    }
  }

  def report(m: QueueMetric): Unit = reporter.foreach(_.report(m))

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
  ) extends QueueSampler

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
  case object Overflown extends Report

}
