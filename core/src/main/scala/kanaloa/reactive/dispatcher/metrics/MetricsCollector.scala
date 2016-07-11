package kanaloa.reactive.dispatcher.metrics

import java.time.{LocalDateTime ⇒ Time}

import akka.actor._
import kanaloa.reactive.dispatcher.metrics.Metric._
import kanaloa.reactive.dispatcher.metrics.MetricsCollector._

import scala.concurrent.duration._
import kanaloa.util.FiniteCollection._
import kanaloa.util.Java8TimeExtensions._
import akka.agent.Agent

class MetricsCollector(
  reporter: Option[Reporter],
  settings: Settings
) extends Actor {

  var subscribers: Set[ActorRef] = Set.empty

  val scheduledSampling = {
    import context.dispatcher
    context.system.scheduler.schedule(
      settings.sampleRate,
      settings.sampleRate,
      self,
      AddSample
    )
  }

  override def postStop(): Unit = {
    scheduledSampling.cancel()
    super.postStop()
  }

  def receive = casual(None)

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
      publish(PartialUtilization(poolSize - idle))
    }
  }

  /**
   *
   * @param s
   * @return
   */
  def busy(s: QueueStatus): Receive = handleSubscriptions orElse {
    case PoolIdle(size) if size > 0 ⇒
      tryComplete(s)
      publishUtilization(size, s.poolSize)
      context become casual(s.poolSize)

    case metric: Metric ⇒
      handle(metric) {
        case WorkCompleted | WorkFailed ⇒
          continue(s.copy(workDone = s.workDone + 1))

        case PoolSize(size) ⇒
          val sizeChanged = s.poolSize.fold(true)(_ != size)
          if (sizeChanged) {
            tryComplete(s)
            continue(QueueStatus(poolSize = Some(size)))
          }
      }

    case AddSample ⇒
      continue(tryComplete(s))

  }

  /**
   * keep it low when the pool isn't fully occupied.
   */
  def casual(poolSize: Option[Int]): Receive = handleSubscriptions orElse {
    case PoolIdle(size) if size == 0 ⇒
      context become busy(QueueStatus(poolSize = poolSize))
    case PoolIdle(idle) ⇒
      publishUtilization(idle, poolSize)
    case metric: Metric ⇒
      handle(metric) {
        case PoolSize(s) ⇒ context become casual(Some(s))
      }
    case AddSample ⇒
  }

  private def continue(qs: QueueStatus): Unit = context become busy(qs)

  /**
   *
   * @param status
   * @return a reset status if completes, the original status if not.
   */
  private def tryComplete(status: QueueStatus): QueueStatus = {
    status.toSample(settings.minSampleDuration).fold(status) { sample ⇒
      publish(sample)
      status.copy(workDone = 0, start = Time.now)
    }
  }

  private def handle(metric: Metric)(pf: PartialFunction[Metric, Unit]): Unit = {
    reporter.foreach(_.report(metric))
    pf.applyOrElse(metric, (_: Metric) ⇒ ())
  }

  def publish(report: Report): Unit = {
    subscribers.foreach(_ ! report)
  }
}

object MetricsCollector {

  def props(
    reporter: Option[Reporter],
    settings: Settings         = Settings()
  ): Props = Props(new MetricsCollector(reporter, settings))

  def apply(
    reporter: Option[Reporter],
    settings: Settings         = Settings()
  )(implicit system: ActorSystem): ActorRef = system.actorOf(props(reporter, settings))

  case object AddSample

  case class Subscribe(actorRef: ActorRef)
  case class Unsubscribe(actorRef: ActorRef)

  /**
   *
   * @param sampleRate
   * @param minSampleDurationRatio minimum sample duration ratio to sample rate. Sample duration less than this will be abandoned.
   */
  case class Settings(
    sampleRate:             FiniteDuration = 1.second,
    minSampleDurationRatio: Double         = 0.3
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
