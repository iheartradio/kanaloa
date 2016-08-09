package kanaloa.reactive.dispatcher.queue

import java.time.{Duration ⇒ JDuration, LocalDateTime ⇒ Time}

import akka.actor._
import kanaloa.reactive.dispatcher.ApiProtocol.QueryStatus
import kanaloa.reactive.dispatcher.PerformanceSampler
import kanaloa.reactive.dispatcher.PerformanceSampler._
import kanaloa.reactive.dispatcher.Types.Speed
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
  var perfLog: PerformanceLogs = Map.empty

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
      perfLog = updateLogs(perfLog, s, weightOfLatestMetric)
      context become fullyUtilized(s.poolSize)

    case PartialUtilization(u) ⇒
      context become underUtilized(u)

    case OptimizeOrExplore ⇒
      val action = {
        if (random.nextDouble() < explorationRatio || perfLog.size < 2)
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
    val newSize = Autothrottler.optimize(currentSize, perfLog, settings)
    ScaleTo(newSize, Some("optimizing"))
  }

  private def explore(currentSize: PoolSize): ScaleTo = {
    val change = Math.max(1, Random.nextInt(maxExploreStepSize))
    ScaleTo(
      currentSize + (if (random.nextDouble() < chanceOfScalingDownWhenFull) -change else change),
      Some("exploring")
    )
  }

  private implicit def durationToJDuration(d: FiniteDuration): JDuration = JDuration.ofNanos(d.toNanos)
}

object Autothrottler {
  case object OptimizeOrExplore

  /**
   * @return change to pool
   */
  private[queue] def optimize(
    currentSize: PoolSize,
    perfLog:     PerformanceLogs,
    settings:    AutothrottleSettings
  ): PoolSize = {
    import settings.{weightOfLatency, optimizationMinRange, optimizationRangeRatio}
    val adjacentPerformances: PerformanceLogs = {
      def adjacency = (size: Int) ⇒ Math.abs(currentSize - size)
      val numOfSizesEachSide = Math.max(optimizationMinRange, (currentSize.toDouble * optimizationRangeRatio).toInt)
      val sizes = perfLog.keys.toSeq
      val leftBoundary = sizes.filter(_ < currentSize).sortBy(adjacency).take(numOfSizesEachSide).lastOption.getOrElse(currentSize)
      val rightBoundary = sizes.filter(_ >= currentSize).sortBy(adjacency).take(numOfSizesEachSide + 1).lastOption.getOrElse(currentSize)
      perfLog.filter { case (size, _) ⇒ size >= leftBoundary && size <= rightBoundary }
    }

    val currentPerf = perfLog.get(currentSize).getOrElse(adjacentPerformances.head._2)
    val normalized = adjacentPerformances.map {
      case (size, that) ⇒
        val speedImprovement = (that.speed.value - currentPerf.speed.value) / currentPerf.speed.value
        val latencyImprovement = (for {
          currentLatency ← currentPerf.processTime
          thatLatency ← that.processTime
        } yield (currentLatency - thatLatency).toNanos.toDouble / currentLatency.toNanos.toDouble) getOrElse 0d

        val improvement = (latencyImprovement * weightOfLatency) + (speedImprovement * (1d - weightOfLatency))
        (size, improvement)
    }
    val optimalSize = normalized.maxBy(_._2)._1

    val scaleStep = Math.ceil((optimalSize - currentSize).toDouble / 2d).toInt

    currentSize + scaleStep
  }

  private[queue] def updateLogs(logs: PerformanceLogs, sample: Sample, weightOfLatestMetric: Double): PerformanceLogs = {
    val existingEntry = logs.get(sample.poolSize)
    val newSpeed = existingEntry.fold(sample.speed) { e ⇒
      Speed(e.speed.value * (1d - weightOfLatestMetric) + (sample.speed.value * weightOfLatestMetric))
    }
    val lastProcessTimeO = existingEntry.flatMap(_.processTime)
    val newProcessTime = (for {
      lastPT ← lastProcessTimeO
      newPT ← sample.avgProcessTime
    } yield lastPT * (1d - weightOfLatestMetric) + (newPT * weightOfLatestMetric)) orElse sample.avgProcessTime orElse lastProcessTimeO

    logs + (sample.poolSize → PerformanceLogEntry(newSpeed, newProcessTime))
  }

  /**
   * Mostly for testing purpose
   */
  private[queue] case class AutothrottleStatus(
    partialUtilization:      Option[Int]      = None,
    partialUtilizationStart: Option[Time]     = None,
    performanceLog:          PerformanceLogs  = Map.empty,
    poolSize:                Option[PoolSize] = None
  )

  type PoolSize = Int

  private[queue] case class PerformanceLogEntry(speed: Speed, processTime: Option[Duration] = None)

  private[queue]type PerformanceLogs = Map[PoolSize, PerformanceLogEntry]

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
