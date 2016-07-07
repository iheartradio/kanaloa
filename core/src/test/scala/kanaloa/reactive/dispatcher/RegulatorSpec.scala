package kanaloa.reactive.dispatcher

import kanaloa.reactive.dispatcher.PerformanceSampler.Sample
import kanaloa.reactive.dispatcher.Regulator.{DroppingRate, Status, Settings}
import kanaloa.reactive.dispatcher.Types.{Speed, QueueLength}
import concurrent.duration._
import DurationFunctions._
import java.time.{LocalDateTime ⇒ Time}

class RegulatorSpec extends SpecWithActorSystem {
  def sample(
    workDone:    Int            = 3,
    queueLength: Int            = 5,
    duration:    FiniteDuration = 1.second
  ): Sample =
    Sample(workDone, duration.ago, Time.now, poolSize = 14, queueLength = QueueLength(queueLength))

  def status(
    lastDelay:         FiniteDuration = 1.second,
    droppingRate:      Double         = 0,
    burstDurationLeft: Duration       = 30.seconds,
    averageSpeed:      Double         = 0.1,
    recordedAt:        Time           = Time.now
  ): Status =
    Status(lastDelay, DroppingRate(droppingRate), burstDurationLeft, Speed(averageSpeed), recordedAt)

  def settings(
    referenceDelay:         FiniteDuration = 3.seconds,
    delayFactorBase:        Double         = 0.5,
    delayTrendFactorBase:   Double         = 0.3,
    durationOfBurstAllowed: FiniteDuration = Duration.Zero,
    weightOfLatestMetric:   Double         = 0.5
  ) =
    Settings(referenceDelay, delayFactorBase, delayTrendFactorBase, durationOfBurstAllowed, weightOfLatestMetric)

  "Regulator" should {
    import Regulator.update

    "update speed with weight" in {
      val lastStatus = status(averageSpeed = 0.2)
      val s = sample(workDone = 100, duration = 1.second) //speed of 0.1
      val result = update(s, lastStatus, settings(weightOfLatestMetric = 0.25))
      result.averageSpeed.value shouldBe 0.175 +- 0.0001 // 0.1 * 0.25 + 0.2 * 0.75
    }

    "calculates delay from weighted average speed" in {
      val delay = update(
        sample(workDone = 100, duration = 1.second, queueLength = 175), //speed of 0.1
        status(averageSpeed = 0.2),
        settings(weightOfLatestMetric = 0.25)
      ).delay

      delay.toMillis shouldBe 1000
    }

    "update p using based factors when p > 10%" in {
      val result = update(
        sample(workDone = 100, duration = 1.second, queueLength = 100), //speed of 0.1
        status(droppingRate = 0.2, averageSpeed = 0.3, lastDelay = 200.milliseconds),
        settings(delayFactorBase = 0.5, delayTrendFactorBase = 0.2, referenceDelay = 400.milliseconds)
      ).droppingRate

      result.value shouldBe 0.475 +- 0.00001
    }

    "update p using half of based factors when 1% < p < 10%" in {
      val result = update(
        sample(workDone = 100, duration = 1.second, queueLength = 100), //speed of 0.1
        status(droppingRate = 0.05, averageSpeed = 0.3, lastDelay = 200.milliseconds),
        settings(delayFactorBase = 0.5, delayTrendFactorBase = 0.2, referenceDelay = 400.milliseconds)
      ).droppingRate

      result.value shouldBe 0.1875 +- 0.00001
    }

    "update p using 1/8 of based factors when p < 1%" in {
      val result = update(
        sample(workDone = 100, duration = 1.second, queueLength = 100), //speed of 0.1
        status(droppingRate = 0.005, averageSpeed = 0.3, lastDelay = 200.milliseconds),
        settings(delayFactorBase = 0.5, delayTrendFactorBase = 0.2, referenceDelay = 400.milliseconds)
      ).droppingRate

      result.value shouldBe 0.039375 +- 0.00001
    }

    "p ceiling at 1" in {
      val result = update(
        sample(workDone = 100, duration = 1.second, queueLength = 1000),
        status(droppingRate = 0.5, averageSpeed = 0.3, lastDelay = 200.milliseconds),
        settings(delayFactorBase = 0.5, delayTrendFactorBase = 0.2, referenceDelay = 100.milliseconds)
      ).droppingRate

      result.value shouldBe 1
    }

    "p floor at 0" in {
      val result = update(
        sample(workDone = 100, duration = 1.second, queueLength = 1), //speed of 0.1
        status(droppingRate = 0.05, averageSpeed = 0.3, lastDelay = 200.milliseconds),
        settings(delayFactorBase = 0.5, delayTrendFactorBase = 0.2, referenceDelay = 500.milliseconds)
      ).droppingRate

      result.value shouldBe 0
    }

    "reset burst allowed duration left when p is 0 and both current and previous delay is less than half of reference delay" in {
      val result = update(
        sample(workDone = 100, duration = 1.second, queueLength = 20), //speed of 0.1 current delay 100
        status(droppingRate = 0.01, averageSpeed = 0.3, lastDelay = 50.milliseconds, burstDurationLeft = 1.second),
        settings(delayFactorBase = 0.5, delayTrendFactorBase = 0.2, referenceDelay = 300.milliseconds, durationOfBurstAllowed = 30.seconds)
      ).burstDurationLeft

      result shouldBe 30.seconds
    }

    "deduct burst allowed duration left when p > 0" in {
      val result = update(
        sample(workDone = 100, duration = 1.second, queueLength = 1000), //speed of 0.1 current delay 5s
        status(droppingRate = 0.5, averageSpeed = 0.3, lastDelay = 50.milliseconds, burstDurationLeft = 28.second, recordedAt = 2.seconds.ago),
        settings(delayFactorBase = 0.5, delayTrendFactorBase = 0.2, referenceDelay = 300.milliseconds, durationOfBurstAllowed = 30.seconds)
      ).burstDurationLeft

      result.toMillis shouldBe 26000
    }

  }
}
