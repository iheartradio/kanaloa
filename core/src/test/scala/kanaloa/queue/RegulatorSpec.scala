package kanaloa.queue

import java.time.{LocalDateTime ⇒ Time}

import akka.testkit.TestProbe
import Regulator._
import kanaloa.Types.{QueueLength, Speed}
import kanaloa.metrics.Metric
import kanaloa.queue.QueueSampler._
import kanaloa.queue.Sampler.Subscribe
import kanaloa.util.Java8TimeExtensions._
import kanaloa.DurationFunctions._
import kanaloa.SpecWithActorSystem
import kanaloa.queue.Regulator.BurstStatus.{BurstExpired, GoingOut, InBurst, OutOfBurst}

import scala.concurrent.duration._

class RegulatorSpec extends SpecWithActorSystem {
  def sample(
    workDone:    Int            = 3,
    queueLength: Int            = 5,
    duration:    FiniteDuration = 1.second
  ): QueueSample =
    QueueSample(workDone, duration.ago, Time.now, queueLength = QueueLength(queueLength))

  def status(
    delay:        FiniteDuration = 1.second,
    droppingRate: Double         = 0,
    averageSpeed: Double         = 0.1,
    burstStatus:  BurstStatus    = OutOfBurst

  ): Status =
    Status(delay, DroppingRate(droppingRate), Speed(averageSpeed), burstStatus)

  def settings(
    referenceDelay:              FiniteDuration = 3.seconds,
    delayFactorBase:             Double         = 0.5,
    delayTrendFactorBase:        Double         = 0.3,
    durationOfBurstAllowed:      FiniteDuration = Duration.Zero,
    weightOfLatestMetric:        Double         = 0.5,
    minDurationBeforeBurstReset: FiniteDuration = Duration.Zero
  ) =
    Settings(referenceDelay, delayFactorBase, delayTrendFactorBase, durationOfBurstAllowed, weightOfLatestMetric, minDurationBeforeBurstReset)

  "Regulator" should {
    import Regulator.update

    "send metrics when seeing samples" in {
      val metricsCollector = TestProbe()
      val regulator = system.actorOf(Regulator.props(settings(), metricsCollector.ref, TestProbe().ref))
      metricsCollector.expectMsgType[Subscribe]
      metricsCollector.expectMsgType[Metric.DropRate].value shouldBe 0d

      regulator ! sample() //starts the regulator
      regulator ! sample()

      val metrics = List(
        metricsCollector.expectMsgType[Metric],
        metricsCollector.expectMsgType[Metric],
        metricsCollector.expectMsgType[Metric]
      )

      metrics.collect {
        case m: Metric.WorkQueueExpectedWaitTime ⇒ 1
        case m: Metric.DropRate                  ⇒ 2
        case m: Metric.BurstMode                 ⇒ 3
        case _                                   ⇒ 0
      }.sorted should be(List(1, 2, 3)) //awkward way to test the receiption of three types of Metrics

      (metrics.collect { case m: Metric.BurstMode ⇒ m }).head.inBurst shouldBe true
    }

    "send metrics when in burst mode" in {
      val metricsCollector = TestProbe()
      val regulator = system.actorOf(Regulator.props(settings(durationOfBurstAllowed = 100.milliseconds), metricsCollector.ref, TestProbe().ref))
      metricsCollector.expectMsgType[Subscribe]
      metricsCollector.expectMsgType[Metric.DropRate].value shouldBe 0d

      regulator ! sample() //starts the regulator
      regulator ! sample(workDone = 1, duration = 10.seconds) //trigger burst mode
      val metrics = List(
        metricsCollector.expectMsgType[Metric],
        metricsCollector.expectMsgType[Metric],
        metricsCollector.expectMsgType[Metric]
      )

      metrics.collect {
        case m: Metric.WorkQueueExpectedWaitTime ⇒ 1
        case m: Metric.DropRate                  ⇒ 2
        case m: Metric.BurstMode                 ⇒ 3
        case _                                   ⇒ 0
      }.sorted should be(List(1, 2, 3))

      metricsCollector.expectNoMsg(60.milliseconds) //wait for a bit to make sure burstLeft is consumed but still have some left

      regulator ! sample(workDone = 1, duration = 10.seconds)

      val metrics2 = List(
        metricsCollector.expectMsgType[Metric],
        metricsCollector.expectMsgType[Metric],
        metricsCollector.expectMsgType[Metric]
      )
      metrics2.collect { case m: Metric.BurstMode ⇒ m }.head.inBurst shouldBe true

      metricsCollector.expectNoMsg(60.milliseconds) //wait to make sure burstLeft is all sued

      regulator ! sample(workDone = 1, duration = 10.seconds)

      val metrics3 = List(
        metricsCollector.expectMsgType[Metric],
        metricsCollector.expectMsgType[Metric],
        metricsCollector.expectMsgType[Metric]
      )
      metrics3.collect { case m: Metric.BurstMode ⇒ m }.head.inBurst shouldBe false
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
      newStatus.delay.toSeconds.toInt shouldBe 175 +- 1
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

      result.value shouldBe 0.1875 +- 0.001
    }

    "update p using 1/8 of based factors when p < 1%" in {
      val result = update(
        sample(workDone = 100, duration = 1.second, queueLength = 100), //speed of 0.1
        status(droppingRate = 0.005, averageSpeed = 0.3, delay = 200.milliseconds),
        settings(delayFactorBase = 0.5, delayTrendFactorBase = 0.2, referenceDelay = 400.milliseconds)
      ).droppingRate

      result.value shouldBe 0.03937 +- 0.0001
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

    "get out of burst left when p is 0 and both current and previous delay is less than half of reference delay" in {
      val started = 1.second.ago
      val result = update(
        sample(workDone = 100, duration = 1.second, queueLength = 20), //speed of 0.1 current delay 100
        status(droppingRate = 0.01, averageSpeed = 0.3, delay = 50.milliseconds, burstStatus = InBurst(started)),
        settings(delayFactorBase = 0.5, delayTrendFactorBase = 0.2, referenceDelay = 300.milliseconds, durationOfBurstAllowed = 30.seconds)
      ).burstStatus

      BurstStatus.inBurstNow(result) shouldBe false
    }

    "reset burst started time if it's a new burst" in {
      val result = update(
        sample(workDone = 100, duration = 1.second, queueLength = 1000), //speed of 0.1 current delay 5s
        status(droppingRate = 0.01, averageSpeed = 0.3, delay = 50.milliseconds, burstStatus = OutOfBurst),
        settings(delayFactorBase = 0.5, delayTrendFactorBase = 0.2, referenceDelay = 300.milliseconds, durationOfBurstAllowed = 30.seconds)
      ).burstStatus

      result match {
        case InBurst(start) ⇒
          start.until(Time.now).toMillis should be < 50L
        case _ ⇒ fail(s"expect InBurst, got $result")
      }
    }

    "continue burst when p > 0" in {
      val lastBurstStarted = 2.second.ago
      val result = update(
        sample(workDone = 100, duration = 1.second, queueLength = 1000), //speed of 0.1 current delay 5s
        status(droppingRate = 0.5, averageSpeed = 0.3, delay = 50.milliseconds, burstStatus = InBurst(lastBurstStarted)),
        settings(delayFactorBase = 0.5, delayTrendFactorBase = 0.2, referenceDelay = 300.milliseconds, durationOfBurstAllowed = 30.seconds)
      ).burstStatus

      result shouldBe InBurst(lastBurstStarted)
    }

    "expire burst when it's already bursting longer than allowed" in {
      val lastBurstStarted = 30.second.ago
      val result = update(
        sample(workDone = 100, duration = 1.second, queueLength = 1000), //speed of 0.1 current delay 5s
        status(droppingRate = 0.5, averageSpeed = 0.3, delay = 50.milliseconds, burstStatus = InBurst(lastBurstStarted)),
        settings(delayFactorBase = 0.5, delayTrendFactorBase = 0.2, referenceDelay = 300.milliseconds, durationOfBurstAllowed = 10.seconds)
      ).burstStatus

      result shouldBe BurstExpired
    }

    "does not reset when not seeing enough caught up " in {
      val lastBurstStarted = 5.second.ago
      val s = settings(delayFactorBase = 0.5, delayTrendFactorBase = 0.2, referenceDelay = 300.milliseconds, durationOfBurstAllowed = 10.seconds, minDurationBeforeBurstReset = 20.milliseconds)

      val firstUpdate = update(
        sample(workDone = 100, duration = 1.second, queueLength = 10),
        status(droppingRate = 0.01, averageSpeed = 3, delay = 50.milliseconds, burstStatus = InBurst(lastBurstStarted)),
        s
      )

      firstUpdate.burstStatus shouldBe a[GoingOut]

      val secondUpdate = update(
        sample(workDone = 100, duration = 1.second, queueLength = 10),
        firstUpdate,
        s
      )
      secondUpdate.burstStatus shouldBe a[GoingOut]

      Thread.sleep(30)
      val thirdUpdate = update(
        sample(workDone = 100, duration = 1.second, queueLength = 10),
        secondUpdate,
        s
      )

      thirdUpdate.burstStatus shouldBe OutOfBurst

    }

    "going back burst it once it sees another burst needed. " in {
      val result = update(
        sample(workDone = 100, duration = 1.second, queueLength = 1000),
        status(droppingRate = 0.5, averageSpeed = 0.3, delay = 50.milliseconds, burstStatus = OutOfBurst),
        settings(delayFactorBase = 0.5, delayTrendFactorBase = 0.2, referenceDelay = 300.milliseconds, durationOfBurstAllowed = 10.seconds, minDurationBeforeBurstReset = 30.seconds)
      ).burstStatus

      result shouldBe a[InBurst]
    }

  }
}
