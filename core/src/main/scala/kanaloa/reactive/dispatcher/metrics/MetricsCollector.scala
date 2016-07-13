package kanaloa.reactive.dispatcher.metrics

import java.time.{LocalDateTime ⇒ Time}

import akka.actor._
import kanaloa.reactive.dispatcher.metrics.Metric._
import kanaloa.reactive.dispatcher.metrics.MetricsCollector._

import scala.concurrent.duration._
import kanaloa.util.FiniteCollection._
import kanaloa.util.Java8TimeExtensions._
import akka.agent.Agent

/**
 * A metrics collector to which all [[Metric]] are sent to.
  *  It forwards incoming [[Metric]]s to [[reporter]].
  *  It collects performance [[Sample]] from [[WorkCompleted]] and [[WorkFailed]]
  *  when the system is in fullyUtilized state, namely when number of idle workers is less than [[MetricsCollectorSettings]]
  *  It can be subscribed using [[Subscribe]] message.
  *  It publishes [[Sample]]s collected to subscribers.\
  *  It also publishes [[PartialUtilization]] number to subscribers.
 * @param reporter
 * @param settings
 */
class MetricsCollector(
  reporter: Option[Reporter],
  settings: MetricsCollectorSettings

) extends Actor {
  import settings._
  var subscribers: Set[ActorRef] = Set.empty

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
      reporter.foreach(_.report(PoolUtilized(utilization)))
    }
  }

  def fullyUtilized(s: QueueStatus): Receive = handleSubscriptions orElse {
    case PoolIdle(size) if size > thresholdOfIdleWorkers ⇒
      tryComplete(s)
      publishUtilization(size, s.poolSize)
      context become partialUtilized(s.poolSize)

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


  def partialUtilized(poolSize: Option[Int]): Receive = handleSubscriptions orElse {
    case PoolIdle(size) if size <= thresholdOfIdleWorkers ⇒
      context become fullyUtilized(QueueStatus(poolSize = poolSize))
    case PoolIdle(idle) ⇒
      publishUtilization(idle, poolSize)
    case metric: Metric ⇒
      handle(metric) {
        case PoolSize(s) ⇒
          context become partialUtilized(Some(s))
      }
    case AddSample ⇒
  }

  private def continue(qs: QueueStatus): Unit = context become fullyUtilized(qs)

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
    settings: MetricsCollectorSettings = MetricsCollectorSettings()
  ): Props = Props(new MetricsCollector(reporter, settings))

  def apply(
    reporter: Option[Reporter],
    settings: MetricsCollectorSettings = MetricsCollectorSettings()
  )(implicit system: ActorSystem): ActorRef = system.actorOf(props(reporter, settings))

  case object AddSample

  case class Subscribe(actorRef: ActorRef)
  case class Unsubscribe(actorRef: ActorRef)

  /**
   *
   * @param sampleRate
   * @param minSampleDurationRatio minimum sample duration ratio to sample rate. Sample duration less than this will be abandoned.
   * @param thresholdOfIdleWorkers The maximum number of idle workers that is allowed for the system to be considered as fully utilized
   */
  case class MetricsCollectorSettings(
    sampleRate:             FiniteDuration = 1.second,
    minSampleDurationRatio: Double         = 0.3,
    thresholdOfIdleWorkers: Int            = 0
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
