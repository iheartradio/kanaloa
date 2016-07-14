package kanaloa.reactive.dispatcher.queue

import java.time.{Duration ⇒ JDuration, LocalDateTime ⇒ Time}

import akka.actor._
import kanaloa.reactive.dispatcher.ApiProtocol.QueryStatus
import kanaloa.reactive.dispatcher.PerformanceSampler.{PartialUtilization, Sample, Subscribe, Unsubscribe}
import kanaloa.reactive.dispatcher.queue.AutoScaling._
import kanaloa.reactive.dispatcher.queue.QueueProcessor.ScaleTo
import kanaloa.util.Java8TimeExtensions._
import kanaloa.util.MessageScheduler

import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.Random

/**
 *  Auto-scales the work pool to an optimal size that provides the highest throughput.
 *  This auto-scaling works best when you expect the pool size to performance function
 *  to be a convex function, with which you can find a global optimal by walking towards
 *  a better size. For example, a CPU bound service may have an optimal worker pool size
 *  tied to the CPU cores available. When your service is IO bound, the optimal size is
 *  bound to optimal number of concurrent connections to that IO service - e.g. a 4 node
 *  Elasticsearch cluster may handle 4-8 concurrent requests at optimal speed.
 *  The dispatchers keep track of throughput at each pool size and perform the following
 *  three resizing operations (one at a time) periodically:
 *  1. Downsize if it hasn't seen all workers ever fully utilized for a period of time.
 *  2. Explore to a random nearby pool size to try and collect throughput metrics.
 *  3. Optimize to a nearby pool size with a better (than any other nearby sizes)
 *     throughput metrics.
 *  When the pool is fully-utilized (i.e. all workers are busy), it randomly chooses
 *  between exploring and optimizing. When the pool has not been fully-utilized for a period of
 *  time, it will downsize the pool to the last seen max utilization multiplied by
 *  a configurable ratio.
 *
 *  By constantly exploring and optimizing, the resizer will eventually walk to the optimal
 *  size and remain nearby.
 *  When the optimal size changes it will start walking towards the new one.
 */
trait AutoScaling extends Actor with ActorLogging with MessageScheduler {
  val processor: QueueProcessorRef
  val metricsCollector: ActorRef
  context watch processor

  val settings: AutoScalingSettings

  import settings._

  var actionScheduler: Option[Cancellable] = None
  var perfLog: PerformanceLog = Map.empty

  override def preStart(): Unit = {
    super.preStart()
    metricsCollector ! Subscribe(self)
    import context.dispatcher
    actionScheduler = Some(context.system.scheduler.schedule(scalingInterval, scalingInterval, self, OptimizeOrExplore))
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

  final def receive: Receive = {
    case s: Sample ⇒
      context become fullyUtilized(s.poolSize)
      self forward s
    case PartialUtilization(u) ⇒
      context become underUtilized(u)
    case OptimizeOrExplore ⇒ //no history no action
  }

  private def underUtilized(highestUtilization: Int, start: Time = Time.now): Receive = watchingQueueAndProcessor orElse {
    case PartialUtilization(utilization) ⇒
      if (highestUtilization < utilization)
        context become underUtilized(utilization, start)
    case s: Sample ⇒
      context become fullyUtilized(s.poolSize)
      self ! s
    case OptimizeOrExplore ⇒
      if (start.isBefore(Time.now.minus(downsizeAfterUnderUtilization)))
        processor ! ScaleTo((highestUtilization * downsizeRatio).toInt, Some("downsizing"))
    case qs: QueryStatus ⇒
      qs.reply(AutoScalingStatus(partialUtilization = Some(highestUtilization), partialUtilizationStart = Some(start)))
  }

  private def fullyUtilized(currentSize: PoolSize): Receive = watchingQueueAndProcessor orElse {
    case Sample(workDone, start, end, poolSize) ⇒

      val speed: Double = workDone.toDouble / start.until(end).toMillis.toDouble
      val toUpdate = perfLog.get(poolSize).fold(speed) { oldSpeed ⇒
        oldSpeed * (1d - weightOfLatestMetric) + (speed * weightOfLatestMetric)
      }
      perfLog += (poolSize → toUpdate)
      context become fullyUtilized(poolSize)

    case PartialUtilization(u) ⇒
      context become underUtilized(u)

    case OptimizeOrExplore ⇒
      val action = {
        if (Random.nextDouble() < explorationRatio)
          explore(currentSize)
        else
          optimize(currentSize)
      }
      processor ! action

    case qs: QueryStatus ⇒
      qs.reply(AutoScalingStatus(poolSize = Some(currentSize), performanceLog = perfLog))
  }

  private def optimize(currentSize: PoolSize): ScaleTo = {

    val adjacentDispatchWaits: PerformanceLog = {
      def adjacency = (size: Int) ⇒ Math.abs(currentSize - size)
      val sizes = perfLog.keys.toSeq
      val numOfSizesEachSide = numOfAdjacentSizesToConsiderDuringOptimization / 2
      val leftBoundary = sizes.filter(_ < currentSize).sortBy(adjacency).take(numOfSizesEachSide).lastOption.getOrElse(currentSize)
      val rightBoundary = sizes.filter(_ >= currentSize).sortBy(adjacency).take(numOfSizesEachSide).lastOption.getOrElse(currentSize)
      perfLog.filter { case (size, _) ⇒ size >= leftBoundary && size <= rightBoundary }
    }

    val optimalSize = adjacentDispatchWaits.maxBy(_._2)._1
    val scaleStep = Math.ceil((optimalSize - currentSize).toDouble / 2.0).toInt
    ScaleTo(currentSize + scaleStep, Some("optimizing"))
  }

  private def explore(currentSize: PoolSize): ScaleTo = {
    val change = Math.max(1, Random.nextInt(Math.ceil(currentSize * exploreStepSize).toInt))
    if (Random.nextDouble() < chanceOfScalingDownWhenFull)
      ScaleTo(currentSize - change, Some("exploring"))
    else
      ScaleTo(currentSize + change, Some("exploring"))
  }

  private implicit def durationToJDuration(d: FiniteDuration): JDuration = JDuration.ofNanos(d.toNanos)
}

object AutoScaling {
  case object OptimizeOrExplore

  /**
   * Mostly for testing purpose
   */
  private[queue] case class AutoScalingStatus(
    partialUtilization:      Option[Int]      = None,
    partialUtilizationStart: Option[Time]     = None,
    performanceLog:          PerformanceLog   = Map.empty,
    poolSize:                Option[PoolSize] = None
  )

  type PoolSize = Int

  private[queue]type PerformanceLog = Map[PoolSize, Double]

  case class Default(
    processor:        QueueProcessorRef,
    settings:         AutoScalingSettings,
    metricsCollector: ActorRef
  ) extends AutoScaling

  def default(
    processor:        QueueProcessorRef,
    settings:         AutoScalingSettings,
    metricsCollector: ActorRef
  ) = Props(Default(processor, settings, metricsCollector)).withDeploy(Deploy.local)
}

