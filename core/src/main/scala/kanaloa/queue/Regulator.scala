package kanaloa.queue

import java.time.{LocalDateTime ⇒ Time}

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import kanaloa.Types.{QueueLength, Speed}
import kanaloa.metrics.Metric
import kanaloa.queue.QueueSampler.{QueueSample, Report}
import kanaloa.queue.Regulator._
import kanaloa.util.Java8TimeExtensions._

import scala.concurrent.duration._
import Regulator.BurstStatus._
/**
 * A traffic regulator based on the PIE algo (Proportional Integral controller Enhanced)
 * suggested in this paper https://www.ietf.org/mail-archive/web/iccrg/current/pdfB57AZSheOH.pdf by Rong Pan and his collaborators.
 * The algo drop request with a probability, here is the pseudocode
 * Every update interval Tupdate
 *   1. Estimation current queueing delay
 *      currentDelay = queueLength / averageDequeueRate
 *   2. Based on current drop probability, p, determine suitable step scales:
 *      if p < 1%       :  α = α΄ / 8, β = β΄ / 8
 *      else if p < 10% :  α = α΄ / 2, β = β΄  2
 *      else            :  α = α΄,  β = β΄
 *   3, Calculate drop probability as:
 *      p = p
 *         + α * (currentDelay - referenceDelay) / referenceDelay
 *         + β * (currentDelay - oldDelay) / referenceDelay
 *   4, Update previous delay sample rate as
 *      OldDelay - currentDelay
 * The regulator allows for a burst, here is the calculation
 *   1. if burstAllowed > 0
 *        enqueue request bypassing random drop
 *   2. upon Tupdate
 *      if p == 0 and currentDelay < referenceDelay / 2 and oldDelay < referenceDelay / 2
 *         burstAllowed = maxBurst
 *      else
 *         burstAllowed = burstAllowed - timePassed (roughly Tupdate)
 *
 * @param metricsCollector [[kanaloa.queue.QueueSampler]] actor that provides Performance samples,
 *                         this also controls the TupdateRate with frequency of samples
 * @param regulatee        [[kanaloa.PushingDispatcher]] actor that receive the dropping probability update
 */
private[kanaloa] class Regulator(settings: Settings, metricsCollector: ActorRef, regulatee: ActorRef) extends Actor with ActorLogging {

  override def preStart(): Unit = {
    super.preStart()
    metricsCollector ! Sampler.Subscribe(self)
    metricsCollector ! Metric.DropRate(0)

  }

  def receive: Receive = {
    case s: QueueSample ⇒
      estimateDelay(s, s.speed).foreach { delay ⇒
        context become regulating(Status(
          delay = delay,
          droppingRate = DroppingRate(0),
          averageSpeed = s.speed,
          burstStatus = OutOfBurst(None)
        ))
      }
    case _: Report ⇒ //ignore other performance report
  }

  private def regulating(status: Status): Receive = {
    case s: QueueSample ⇒
      continueWith(update(s, status, settings))
    case _: Report ⇒ //ignore other performance report
  }

  private def continueWith(status: Status): Unit = {
    context become regulating(status)
    metricsCollector ! Metric.WorkQueueExpectedWaitTime(status.delay)
    val inBurst = inBurstNow(status.burstStatus)
    metricsCollector ! Metric.BurstMode(inBurst)
    val droppingRateToSend =
      if (inBurst)
        DroppingRate(0)
      else status.droppingRate
    metricsCollector ! Metric.DropRate(droppingRateToSend.value)

    regulatee ! droppingRateToSend
  }
}

object Regulator {

  def props(settings: Settings, sampler: ActorRef, regulatee: ActorRef) =
    Props(new Regulator(settings, sampler, regulatee))

  class DroppingRate(val value: Double) extends AnyVal with Serializable

  object DroppingRate {
    def apply(value: Double): DroppingRate =
      if (value > 1) new DroppingRate(1)
      else if (value < 0) new DroppingRate(0)
      else new DroppingRate(value)
  }

  private[kanaloa] case class Status(
    delay:        FiniteDuration,
    droppingRate: DroppingRate,
    averageSpeed: Speed,
    burstStatus:  BurstStatus
  )

  sealed trait BurstStatus extends Product with Serializable

  object BurstStatus {
    case class InBurst(started: Time) extends BurstStatus
    case class BurstExpired(started: Time) extends BurstStatus
    case class OutOfBurst(latestBurstStarted: Option[Time]) extends BurstStatus

    def latestBurstStarted(status: BurstStatus): Option[Time] = status match {
      case InBurst(started)      ⇒ Some(started)
      case BurstExpired(started) ⇒ Some(started)
      case OutOfBurst(l)         ⇒ l
    }

    def inBurstNow(status: BurstStatus): Boolean = status match {
      case InBurst(_) ⇒ true
      case _          ⇒ false
    }

    def transitOut(status: BurstStatus): OutOfBurst = status match {
      case InBurst(started)      ⇒ OutOfBurst(Some(started))
      case BurstExpired(started) ⇒ OutOfBurst(Some(started))
      case o @ OutOfBurst(_)     ⇒ o
    }

    def continueBurst(status: BurstStatus, durationAllowed: FiniteDuration, minDurationBetweenBursts: FiniteDuration): BurstStatus = status match {
      case o @ OutOfBurst(last) if last.fold(false)(_.until(Time.now) < minDurationBetweenBursts) ⇒ o
      case OutOfBurst(_) ⇒ InBurst(Time.now)
      case InBurst(started) if started.until(Time.now) > durationAllowed ⇒ BurstExpired(started)
      case s @ (InBurst(_) | BurstExpired(_)) ⇒ s
    }

  }

  case class Settings(
    referenceDelay:           FiniteDuration,
    delayFactorBase:          Double,
    delayTrendFactorBase:     Double,
    durationOfBurstAllowed:   FiniteDuration = Duration.Zero,
    weightOfLatestMetric:     Double         = 0.5,
    minDurationBetweenBursts: FiniteDuration = Duration.Zero
  )

  private def estimateDelay(sample: QueueSample, avgSpeed: Speed): Option[FiniteDuration] =
    if (avgSpeed.value == 0 && sample.queueLength.value > 0)
      Some((sample.start.until(sample.end) * sample.queueLength.value.toDouble).asInstanceOf[FiniteDuration])
    else if (avgSpeed.value > 0 && sample.queueLength.value == 0)
      Some(Duration.Zero)
    else if (avgSpeed.value == 0 && sample.queueLength.value == 0)
      None
    else
      Some((sample.queueLength.value.toDouble / avgSpeed.value).milliseconds)

  private[kanaloa] def update(sample: QueueSample, lastStatus: Status, settings: Settings): Status = {
    import settings._
    val avgSpeed = if (sample.queueLength.value > 0) //ignore the speed when queue is empty
      Speed(
        sample.speed.value * weightOfLatestMetric + ((1d - weightOfLatestMetric) * lastStatus.averageSpeed.value)
      )
    else lastStatus.averageSpeed

    estimateDelay(sample, avgSpeed).fold(lastStatus) { delay ⇒

      def normalizedDelayDiffFrom(target: FiniteDuration) = (delay - target) / referenceDelay

      val factorAdjustment = if (lastStatus.droppingRate.value >= 0.1) 1
      else if (lastStatus.droppingRate.value < 0.1 && lastStatus.droppingRate.value >= 0.01)
        0.5
      else 0.125 //these hardcoded numbers are from the paper

      val droppingRateUpdate = (factorAdjustment * delayFactorBase * normalizedDelayDiffFrom(referenceDelay)) +
        (factorAdjustment * delayTrendFactorBase * normalizedDelayDiffFrom(lastStatus.delay))

      val newDropRate = DroppingRate(lastStatus.droppingRate.value + droppingRateUpdate)

      val queueCaughtUp = newDropRate.value == 0 &&
        lastStatus.delay < (referenceDelay / 2) &&
        delay < (referenceDelay / 2)

      val newBurstStatus: BurstStatus = if (queueCaughtUp) {
        transitOut(lastStatus.burstStatus)
      } else {
        continueBurst(lastStatus.burstStatus, durationOfBurstAllowed, minDurationBetweenBursts)
      }

      lastStatus.copy(
        delay = delay,
        droppingRate = newDropRate,
        averageSpeed = avgSpeed,
        burstStatus = newBurstStatus
      )
    }
  }

}
