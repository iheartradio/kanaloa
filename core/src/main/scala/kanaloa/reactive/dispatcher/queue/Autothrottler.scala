package kanaloa.reactive.dispatcher.queue

import java.time.{Duration ⇒ JDuration, LocalDateTime ⇒ Time}

import akka.actor._
import kanaloa.reactive.dispatcher.ApiProtocol.QueryStatus
import kanaloa.reactive.dispatcher.PerformanceSampler
import kanaloa.reactive.dispatcher.PerformanceSampler._
import kanaloa.reactive.dispatcher.queue.Autothrottler._
import kanaloa.reactive.dispatcher.queue.QueueProcessor.ScaleTo
import kanaloa.util.Java8TimeExtensions._
import kanaloa.util.MessageScheduler

import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.Random

/**
 * For mechanisms see docs in the autothrottle section in reference.conf
 */
trait Autothrottler extends Actor with ActorLogging with MessageScheduler {
  val processor: QueueProcessorRef
  val metricsCollector: ActorRef

  val settings: AutothrottleSettings

  import settings._

  val random: Random = new Random(23)
  var actionScheduler: Option[Cancellable] = None
  var perfLog: PerformanceLog = Map.empty

  override def preStart(): Unit = {
    super.preStart()
    metricsCollector ! Subscribe(self)
    context watch processor
    import context.dispatcher
    actionScheduler = Some(context.system.scheduler.schedule(resizeInterval, resizeInterval, self, OptimizeOrExplore))
  }

  override def postStop(): Unit = {
    super.postStop()
    actionScheduler.map(_.cancel())
    metricsCollector ! Unsubscribe(self)
  }

  private def watchingQueueAndProcessor: Receive = {
    case Terminated(`processor`) | QueueProcessor.ShuttingDown ⇒ {
      context stop self
    }
  }

  final def receive: Receive = watchingQueueAndProcessor orElse {
    case s: Sample if s.poolSize > 0 ⇒
      context become fullyUtilized(s.poolSize)
      self forward s
    case PartialUtilization(u) ⇒
      context become underUtilized(u)
    case OptimizeOrExplore            ⇒ //no history no action
    case _: PerformanceSampler.Report ⇒ //ignore other performance report
  }

  private def underUtilized(highestUtilization: Int, start: Time = Time.now): Receive = watchingQueueAndProcessor orElse {
    case PartialUtilization(utilization) ⇒
      if (highestUtilization < utilization)
        context become underUtilized(utilization, start)
    case s: Sample if s.poolSize > 0 ⇒
      context become fullyUtilized(s.poolSize)
      self forward s
    case OptimizeOrExplore ⇒
      if (start.isBefore(Time.now.minus(downsizeAfterUnderUtilization)))
        processor ! ScaleTo((highestUtilization * downsizeRatio).toInt, Some("downsizing"))
    case qs: QueryStatus ⇒
      qs.reply(AutothrottleStatus(partialUtilization = Some(highestUtilization), partialUtilizationStart = Some(start)))

    case _: PerformanceSampler.Report ⇒ //ignore other performance report
  }

  private def fullyUtilized(currentSize: PoolSize): Receive = watchingQueueAndProcessor orElse {
    case s: Sample if s.poolSize > 0 ⇒
      val toUpdate = perfLog.get(s.poolSize).fold(s.speed.value) { oldSpeed ⇒
        oldSpeed * (1d - weightOfLatestMetric) + (s.speed.value * weightOfLatestMetric)
      }
      perfLog += (s.poolSize → toUpdate)
      context become fullyUtilized(s.poolSize)

    case PartialUtilization(u) ⇒
      context become underUtilized(u)

    case OptimizeOrExplore ⇒
      val action = {
        if (random.nextDouble() < explorationRatio)
          explore(currentSize)
        else
          optimize(currentSize)
      }
      processor ! action

    case qs: QueryStatus ⇒
      qs.reply(AutothrottleStatus(poolSize = Some(currentSize), performanceLog = perfLog))

    case _: PerformanceSampler.Report ⇒ //ignore other performance report
  }

  private def optimize(currentSize: PoolSize): ScaleTo = {

    val adjacentPerformances: PerformanceLog = {
      def adjacency = (size: Int) ⇒ Math.abs(currentSize - size)
      val sizes = perfLog.keys.toSeq
      val numOfSizesEachSide = numOfAdjacentSizesToConsiderDuringOptimization / 2
      val leftBoundary = sizes.filter(_ < currentSize).sortBy(adjacency).take(numOfSizesEachSide).lastOption.getOrElse(currentSize)
      val rightBoundary = sizes.filter(_ >= currentSize).sortBy(adjacency).take(numOfSizesEachSide).lastOption.getOrElse(currentSize)
      perfLog.filter { case (size, _) ⇒ size >= leftBoundary && size <= rightBoundary }
    }

    log.debug("Optimizing based on performance table: " +
      adjacentPerformances.toList.sortBy(_._2).takeRight(10).reverse.map(p ⇒ s"Sz: ${p._1} spd: ${p._2 * 100} ").mkString(" | "))

    val optimalSize = adjacentPerformances.maxBy(_._2)._1

    val scaleStep = Math.ceil((optimalSize - currentSize).toDouble / 2.0).toInt

    if (scaleStep > 0)
      log.debug("Optimized to " + (currentSize + scaleStep) + " from " + currentSize)

    ScaleTo(currentSize + scaleStep, Some("optimizing"))
  }

  private def explore(currentSize: PoolSize): ScaleTo = {
    val change = Math.max(1, Random.nextInt(Math.ceil(currentSize * exploreStepSize).toInt))
    if (random.nextDouble() < chanceOfScalingDownWhenFull)
      ScaleTo(currentSize - change, Some("exploring"))
    else
      ScaleTo(currentSize + change, Some("exploring"))
  }

  private implicit def durationToJDuration(d: FiniteDuration): JDuration = JDuration.ofNanos(d.toNanos)
}

object Autothrottler {
  case object OptimizeOrExplore

  /**
   * Mostly for testing purpose
   */
  private[queue] case class AutothrottleStatus(
    partialUtilization:      Option[Int]      = None,
    partialUtilizationStart: Option[Time]     = None,
    performanceLog:          PerformanceLog   = Map.empty,
    poolSize:                Option[PoolSize] = None
  )

  type PoolSize = Int

  private[queue]type PerformanceLog = Map[PoolSize, Double]

  case class Default(
    processor:        QueueProcessorRef,
    settings:         AutothrottleSettings,
    metricsCollector: ActorRef
  ) extends Autothrottler

  def default(
    processor:        QueueProcessorRef,
    settings:         AutothrottleSettings,
    metricsCollector: ActorRef
  ) = Props(Default(processor, settings, metricsCollector)).withDeploy(Deploy.local)
}

