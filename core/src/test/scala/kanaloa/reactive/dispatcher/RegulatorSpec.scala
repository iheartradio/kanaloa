package kanaloa.reactive.dispatcher

import akka.testkit.TestProbe
import kanaloa.reactive.dispatcher.PerformanceSampler.{PartialUtilization, Subscribe, Sample}
import kanaloa.reactive.dispatcher.Regulator.{DroppingRate, Status, Settings}
import kanaloa.reactive.dispatcher.Types.{Speed, QueueLength}
import kanaloa.reactive.dispatcher.metrics.Metric
import concurrent.duration._
import DurationFunctions._
import java.time.{LocalDateTime ⇒ Time}
import kanaloa.util.Java8TimeExtensions._

class RegulatorSpec extends SpecWithActorSystem {
  def sample(
    workDone:    Int            = 3,
    queueLength: Int            = 5,
    duration:    FiniteDuration = 1.second
  ): Sample =
    Sample(workDone, duration.ago, Time.now, poolSize = 14, queueLength = QueueLength(queueLength), None)

  def status(
    delay:             FiniteDuration = 1.second,
    droppingRate:      Double         = 0,
    burstDurationLeft: Duration       = 30.seconds,
    averageSpeed:      Double         = 0.1,
    recordedAt:        Time           = Time.now
  ): Status =
    Status(delay, DroppingRate(droppingRate), burstDurationLeft, Speed(averageSpeed), recordedAt)

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

    "send metrics when seeing samples" in {
      val metricsCollector = TestProbe()
      val regulator = system.actorOf(Regulator.props(settings(), metricsCollector.ref, TestProbe().ref))
      metricsCollector.expectMsgType[Subscribe]
      metricsCollector.expectMsgType[Metric.DropRate].value shouldBe 0d

      regulator ! sample() //starts the regulator
      regulator ! sample()

      metricsCollector.expectMsgType[Metric.WorkQueueExpectedWaitTime]
      metricsCollector.expectMsgType[Metric.DropRate]
      metricsCollector.expectMsgType[Metric.BurstMode].inBurst shouldBe false
    }

    "send metrics when in burst mode" in {
      val metricsCollector = TestProbe()
      val regulator = system.actorOf(Regulator.props(settings(durationOfBurstAllowed = 100.milliseconds), metricsCollector.ref, TestProbe().ref))
      metricsCollector.expectMsgType[Subscribe]
      metricsCollector.expectMsgType[Metric.DropRate].value shouldBe 0d

      regulator ! sample() //starts the regulator
      regulator ! sample(workDone = 1, duration = 10.seconds) //trigger burst mode
      metricsCollector.expectMsgType[Metric.WorkQueueExpectedWaitTime]
      metricsCollector.expectMsgType[Metric.DropRate]
      metricsCollector.expectMsgType[Metric.BurstMode]

      metricsCollector.expectNoMsg(60.milliseconds) //wait for a bit to make sure burstLeft is consumed but still have some left

      regulator ! sample(workDone = 1, duration = 10.seconds)

      metricsCollector.expectMsgType[Metric.WorkQueueExpectedWaitTime]
      metricsCollector.expectMsgType[Metric.DropRate]
      metricsCollector.expectMsgType[Metric.BurstMode].inBurst shouldBe true

      metricsCollector.expectNoMsg(60.milliseconds) //wait to make sure burstLeft is all sued

      regulator ! sample(workDone = 1, duration = 10.seconds)

      metricsCollector.expectMsgType[Metric.WorkQueueExpectedWaitTime]
      metricsCollector.expectMsgType[Metric.DropRate]
      metricsCollector.expectMsgType[Metric.BurstMode].inBurst shouldBe false
    }

    "send metrics when seeing PartialUtilization" in {
      val metricsCollector = TestProbe()
      val regulator = system.actorOf(Regulator.props(settings(), metricsCollector.ref, TestProbe().ref))
      metricsCollector.expectMsgType[Subscribe]
      metricsCollector.expectMsgType[Metric.DropRate]

      regulator ! sample() //starts the regulator

      regulator ! PartialUtilization(0)

      metricsCollector.expectMsgType[Metric.WorkQueueExpectedWaitTime].duration.toMillis shouldBe 333
      metricsCollector.expectMsgType[Metric.DropRate].value shouldBe 0d
      metricsCollector.expectMsgType[Metric.BurstMode].inBurst shouldBe false
    }

    "send dropRate when outside burst duration" in {
      val regulatee = TestProbe()
      val regulator = system.actorOf(Regulator.props(settings(durationOfBurstAllowed = 30.milliseconds), TestProbe().ref, regulatee.ref))
      regulator ! sample() //starts the regulator

      regulator ! sample(workDone = 1, queueLength = 10000)
      regulatee.expectMsgType[DroppingRate].value shouldBe (0) //does not send dropping rate larger than zero when within a burst

      regulatee.expectNoMsg(40.milliseconds)
      regulator ! sample(workDone = 1, queueLength = 10000)
      regulatee.expectMsgType[DroppingRate].value should be > 0d //send dropping rate when burst allowed used up.
    }

    "reset drop rate after seeing a PartialUtilization" in {
      val regulatee = TestProbe()
      val regulator = system.actorOf(Regulator.props(settings(), TestProbe().ref, regulatee.ref))
      regulator ! sample() //starts the regulator

      regulator ! sample(workDone = 1, queueLength = 10000)
      regulatee.expectMsgType[DroppingRate].value shouldBe 1d //does not send dropping rate larger than zero when within a burst

      regulator ! PartialUtilization(3)
      regulatee.expectMsgType[DroppingRate].value shouldBe 0d //does not send dropping rate larger than zero when within a burst

    }

    "update recordedAt" in {
      val lastStatus = status(averageSpeed = 0.2, recordedAt = 2000.milliseconds.ago)
      val result = update(sample(), lastStatus, settings())
      val distance = lastStatus.recordedAt.until(result.recordedAt).toMillis.toDouble
      distance shouldBe 2000.0 +- 30
    }

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

    "calculates status from zero speed sample" in {
      val newStatus = update(
        sample(workDone = 0, duration = 1.second, queueLength = 175), //speed of 0.1
        status(averageSpeed = 0),
        settings()
      )

      newStatus.averageSpeed.value shouldBe 0
      newStatus.delay shouldBe 175.seconds
    }

    "update p using based factors when p > 10%" in {
      val result = update(
        sample(workDone = 100, duration = 1.second, queueLength = 100), //speed of 0.1
        status(droppingRate = 0.2, averageSpeed = 0.3, delay = 200.milliseconds),
        settings(delayFactorBase = 0.5, delayTrendFactorBase = 0.2, referenceDelay = 400.milliseconds)
      ).droppingRate

      result.value shouldBe 0.475 +- 0.001
    }

    "update p using half of based factors when 1% < p < 10%" in {
      val result = update(
        sample(workDone = 100, duration = 1.second, queueLength = 100), //speed of 0.1
        status(droppingRate = 0.05, averageSpeed = 0.3, delay = 200.milliseconds),
        settings(delayFactorBase = 0.5, delayTrendFactorBase = 0.2, referenceDelay = 400.milliseconds)
      ).droppingRate

      result.value shouldBe 0.1875 +- 0.00001
    }

    "update p using 1/8 of based factors when p < 1%" in {
      val result = update(
        sample(workDone = 100, duration = 1.second, queueLength = 100), //speed of 0.1
        status(droppingRate = 0.005, averageSpeed = 0.3, delay = 200.milliseconds),
        settings(delayFactorBase = 0.5, delayTrendFactorBase = 0.2, referenceDelay = 400.milliseconds)
      ).droppingRate

      result.value shouldBe 0.039375 +- 0.00001
    }

    "p ceiling at 1" in {
      val result = update(
        sample(workDone = 100, duration = 1.second, queueLength = 1000),
        status(droppingRate = 0.5, averageSpeed = 0.3, delay = 200.milliseconds),
        settings(delayFactorBase = 0.5, delayTrendFactorBase = 0.2, referenceDelay = 100.milliseconds)
      ).droppingRate

      result.value shouldBe 1
    }

    "p floor at 0" in {
      val result = update(
        sample(workDone = 100, duration = 1.second, queueLength = 1), //speed of 0.1
        status(droppingRate = 0.05, averageSpeed = 0.3, delay = 200.milliseconds),
        settings(delayFactorBase = 0.5, delayTrendFactorBase = 0.2, referenceDelay = 500.milliseconds)
      ).droppingRate

      result.value shouldBe 0
    }

    "reset burst allowed duration left when p is 0 and both current and previous delay is less than half of reference delay" in {
      val result = update(
        sample(workDone = 100, duration = 1.second, queueLength = 20), //speed of 0.1 current delay 100
        status(droppingRate = 0.01, averageSpeed = 0.3, delay = 50.milliseconds, burstDurationLeft = 1.second),
        settings(delayFactorBase = 0.5, delayTrendFactorBase = 0.2, referenceDelay = 300.milliseconds, durationOfBurstAllowed = 30.seconds)
      ).burstDurationLeft

      result shouldBe 30.seconds
    }

    "deduct burst allowed duration left when p > 0" in {
      val result = update(
        sample(workDone = 100, duration = 1.second, queueLength = 1000), //speed of 0.1 current delay 5s
        status(droppingRate = 0.5, averageSpeed = 0.3, delay = 50.milliseconds, burstDurationLeft = 28.second, recordedAt = 2.seconds.ago),
        settings(delayFactorBase = 0.5, delayTrendFactorBase = 0.2, referenceDelay = 300.milliseconds, durationOfBurstAllowed = 30.seconds)
      ).burstDurationLeft

      result.toMillis.toDouble should be(26000.0 +- 2) //somehow exact matching integer fails range test.
    }

  }
}
