package kanaloa.reactive.dispatcher.metrics

import java.time.{LocalDateTime ⇒ Time}

import akka.actor._
import kanaloa.reactive.dispatcher.metrics.Metric._
import kanaloa.reactive.dispatcher.metrics.MetricsCollector._

import scala.concurrent.duration._
import kanaloa.util.Java8TimeExtensions._

/**
 *  A metrics collector to which all [[Metric]] are sent to.
 *  It proxy the incoming [[Metric]]s to [[reporter]].
 *  Internally it collects performance [[Sample]] from [[WorkCompleted]] and [[WorkFailed]]
 *  when the system is in fullyUtilized state, namely when number
 *  of idle workers is less than [[MetricsCollectorSettings]]
 *  It internally publishes these [[Sample]]s as well as [[PartialUtilization]] data
 *  which are only for internal tuning purpose, and should not be
 *  confused with the [[Metric]] used for realtime monitoring.
 *  It can be subscribed using [[Subscribe]] message.
 *  It publishes [[Sample]]s and [[PartialUtilization]] number to subscribers.
 * @param reporter
 * @param settings
 */
private[metrics] class MetricsCollector(
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

  def reportQueueLength(workLeft: Int): Unit =
    reporter.foreach(_.report(WorkQueueLength(workLeft)))

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
      for (r ← reporter; s ← s.poolSize) r.report(PoolUtilized(s)) //take the chance to report utilization to reporter

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

private[kanaloa] object MetricsCollector {

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

  case class DispatchResult(workersLeft: Int, workLeft: Int)

  /**
   *
   * @param sampleRate
   * @param minSampleDurationRatio minimum sample duration ratio to sample rate. Sample duration less than this will be abandoned.
   * @param fullyUtilized a function that given the number of idle workers indicate if the pool is fully utilized
   */
  case class MetricsCollectorSettings(
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
