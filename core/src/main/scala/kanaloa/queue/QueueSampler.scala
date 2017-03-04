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
 *  It publishes [[kanaloa.queue.QueueSampler.QueueSample]]s and
 *  [[kanaloa.queue.QueueSampler.PartialUtilized]] and
 *  [[kanaloa.queue.QueueSampler.Overflown]] number to subscribers.
 *
 */
private[kanaloa] trait QueueSampler extends Sampler {

  import settings._

  def reporter: Option[Reporter]

  def receive = monitoring(QueueState(QueueLength(0), PartialUtilizing))

  private def reportQueueLength(queueLength: QueueLength): Unit =
    report(WorkQueueLength(queueLength.value))

  private def monitoring(s: QueueState): Receive = {
    def continue(status: Queue.Status, dispatched: Option[Int]): Unit = {
      val Queue.Status(_, workLeft, workBuffered) = status
      reportQueueLength(workLeft)
      val (event, newOverflowState) = {
        if (workBuffered)
          OverflowState.sawWorkBuffered(s.overflowState)
        else OverflowState.seeingEmptyWorkBuffer(s.overflowState, sampleInterval)
      }
      event.foreach(publish)
      context become monitoring(s.copy(
        queueLength = workLeft,
        workDone = s.workDone + dispatched.getOrElse(0),
        overflowState = newOverflowState
      ))
    }

    handleSubscriptions orElse {
      case r: Queue.DispatchReport ⇒
        continue(r.status, Some(r.dispatched))

      case qs: Queue.Status ⇒
        continue(qs, None)

      case AddSample ⇒
        val sample = s.toSample(minSampleDuration)
        val newState = if (sample.fold(false)(_.dequeued > 0))
          s.copy(workDone = 0, start = Time.now, avgProcessTime = None) //if sample is valid and there is work done restart the counter
        else s

        sample foreach publish
        context become monitoring(newState)

      case metric: QueueMetric ⇒
        report(metric)

      case AddSample ⇒ //do not try to sample because s.bufferEmpty is defined
    }
  }

  private def report(m: QueueMetric): Unit = reporter.foreach(_.report(m))

}

private[kanaloa] object QueueSampler {

  class QueueSamplerImpl(
    val reporter:              Option[Reporter],
    val settings:              SamplerSettings,
    override val autoSampling: Boolean
  ) extends QueueSampler

  def props(
    reporter:     Option[Reporter],
    settings:     SamplerSettings  = SamplerSettings(),
    autoSampling: Boolean          = true
  ): Props = Props(new QueueSamplerImpl(reporter, settings, autoSampling))

  sealed trait OverflowState extends Product with Serializable

  case class BeginToSeeEmptyBuffer(since: Time) extends OverflowState
  case object Overflowing extends OverflowState
  case object PartialUtilizing extends OverflowState

  object OverflowState {
    def sawWorkBuffered(state: OverflowState): (Option[Event], OverflowState) = state match {
      case Overflowing | BeginToSeeEmptyBuffer(_) ⇒ (None, Overflowing)
      case PartialUtilizing                       ⇒ (Some(Overflown), Overflowing)
    }

    def seeingEmptyWorkBuffer(state: OverflowState, threshold: FiniteDuration): (Option[Event], OverflowState) = state match {
      case PartialUtilizing ⇒ (None, PartialUtilizing)
      case Overflowing      ⇒ (None, BeginToSeeEmptyBuffer(Time.now))
      case BeginToSeeEmptyBuffer(since) if since.until(Time.now) > threshold ⇒
        (Some(PartialUtilized), PartialUtilizing)
      case b @ BeginToSeeEmptyBuffer(_) ⇒ (None, b)
    }
  }

  /**
   * Status of the current traffic oversaturation
   */
  final case class QueueState(
    queueLength:    QueueLength,
    overflowState:  OverflowState,
    workDone:       Int              = 0,
    start:          Time             = Time.now,
    avgProcessTime: Option[Duration] = None
  ) extends {

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

  final case class QueueSample(
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

  trait Event extends Report
  case object PartialUtilized extends Event
  case object Overflown extends Event

}
