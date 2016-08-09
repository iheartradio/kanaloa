package kanaloa.reactive.dispatcher.queue

import akka.actor.{ActorRef, ActorSystem, PoisonPill, Terminated}
import akka.testkit._
import kanaloa.reactive.dispatcher.ApiProtocol.QueryStatus
import kanaloa.reactive.dispatcher.DurationFunctions._
import kanaloa.reactive.dispatcher.PerformanceSampler.{PartialUtilization, Sample}
import kanaloa.reactive.dispatcher.Types.{Speed, QueueLength}
import kanaloa.reactive.dispatcher.metrics.MetricsCollector
import kanaloa.reactive.dispatcher.queue.Autothrottler._
import kanaloa.reactive.dispatcher.queue.QueueProcessor.{ScaleTo, Shutdown}
import kanaloa.reactive.dispatcher.queue.Worker.{Idle, Working}
import kanaloa.reactive.dispatcher.{ResultChecker, ScopeWithActor, SpecWithActorSystem}
import org.scalatest.OptionValues
import org.scalatest.concurrent.Eventually
import org.scalatest.mock.MockitoSugar

import scala.concurrent.duration._
import scala.util.Random
import org.mockito.Mockito._

class AutothrottleSpec extends SpecWithActorSystem with OptionValues with Eventually with MockitoSugar {

  def sample(poolSize: PoolSize, avgProcessTime: Option[Duration] = None, workDone: Int = 3) =
    Sample(workDone, 2.second.ago, 1.second.ago, poolSize, QueueLength(14), avgProcessTime)

  "Autothrottle" should {
    "when no history" in new AutothrottleScope {
      as ! OptimizeOrExplore
      tProcessor.expectNoMsg(50.milliseconds)
    }

    "record perfLog" in new AutothrottleScope {
      as ! sample(poolSize = 30)
      as ! QueryStatus()
      val status = expectMsgType[AutothrottleStatus]
      status.poolSize should contain(30)
      status.performanceLog.keys should contain(30)
    }

    "record perfLog with avg process time" in new AutothrottleScope {
      val target = autothrottlerRef(defaultSettings.copy(weightOfLatestMetric = 0.5))
      as ! sample(poolSize = 30, avgProcessTime = Some(25.milliseconds))
      as ! sample(poolSize = 31, avgProcessTime = Some(20.milliseconds))
      as ! sample(poolSize = 31, avgProcessTime = Some(40.milliseconds))
      as ! sample(poolSize = 31, avgProcessTime = Some(90.milliseconds))
      as ! QueryStatus()
      val status = expectMsgType[AutothrottleStatus]

      status.performanceLog(30).processTime should contain(25.milliseconds)
      status.performanceLog(31).processTime should contain(60.milliseconds)
    }

    "update poolsize" in new AutothrottleScope {
      as ! sample(poolSize = 30)
      as ! sample(poolSize = 33)
      as ! sample(poolSize = 35)
      as ! QueryStatus()
      val status = expectMsgType[AutothrottleStatus]
      status.poolSize should contain(35)
      status.performanceLog.keys should contain(33)
    }

    "start an underutilizationStreak" in new AutothrottleScope {
      as ! PartialUtilization(3)
      as ! QueryStatus()
      val status = expectMsgType[AutothrottleStatus]
      status.partialUtilization should contain(3)
      status.partialUtilizationStart should not be (empty)
    }

    "stop an underutilizationStreak" in new AutothrottleScope {
      as ! PartialUtilization(3)
      as ! sample(poolSize = 30)

      as ! QueryStatus()
      val status = expectMsgType[AutothrottleStatus]
      status.partialUtilization should be(empty)
      status.partialUtilizationStart should be(empty)
    }

    "update an underutilizationStreak to the highest utilization" in new AutothrottleScope {
      as ! PartialUtilization(3)
      as ! QueryStatus()

      val status1 = expectMsgType[AutothrottleStatus]

      as ! PartialUtilization(5)
      as ! QueryStatus()

      val status2 = expectMsgType[AutothrottleStatus]

      as ! PartialUtilization(4)
      as ! QueryStatus()

      val status3 = expectMsgType[AutothrottleStatus]

      status1.partialUtilizationStart should not be (empty)
      status1.partialUtilizationStart should be(status3.partialUtilizationStart)
      status3.partialUtilization should contain(5)
    }

    "explore when currently maxed out and exploration rate is 1" in new AutothrottleScope {
      val subject = autothrottlerRef(alwaysExploreSettings)
      subject ! sample(poolSize = 30)

      subject ! OptimizeOrExplore

      val scaleCmd = tProcessor.expectMsgType[ScaleTo]

      scaleCmd.reason.value shouldBe "exploring"
    }

    "does not optimize when not currently maxed" in new AutothrottleScope {
      val subject = autothrottlerRef()
      subject ! sample(poolSize = 30)

      subject ! OptimizeOrExplore
      tProcessor.expectMsgType[ScaleTo]

      subject ! PartialUtilization(4)

      subject ! OptimizeOrExplore

      tProcessor.expectNoMsg(30.millisecond)
    }

    "optimize towards the faster size when currently maxed out and exploration rate is 0" in new AutothrottleScope {
      val subject = autothrottlerRef(alwaysOptimizeSettings)
      mockBusyHistory(
        subject,
        (30, 3),
        (35, 4),
        (40, 9),
        (40, 8),
        (45, 4)
      )
      subject ! OptimizeOrExplore
      val scaleCmd = tProcessor.expectMsgType[ScaleTo]

      scaleCmd.reason.value shouldBe "optimizing"
      scaleCmd.numOfWorkers should be > 35
      scaleCmd.numOfWorkers should be < 45
    }

    "optimize towards better latency when throughput plateau" in {
      val logs = Map(
        20 → PerformanceLogEntry(Speed(22), Some(70.milliseconds)),
        22 → PerformanceLogEntry(Speed(22), Some(100.milliseconds)),
        24 → PerformanceLogEntry(Speed(23), Some(110.milliseconds))
      )
      val result = Autothrottler.optimize(22, logs, AutothrottleSettings(weightOfLatency = 0.3))
      result shouldBe <(22)
      result shouldBe >=(20)
    }

    "optimize towards a more distant one at larger pool size" in {
      val logs = Map(
        20 → PerformanceLogEntry(Speed(30)),
        21 → PerformanceLogEntry(Speed(22)),
        22 → PerformanceLogEntry(Speed(21)),
        23 → PerformanceLogEntry(Speed(25)),
        24 → PerformanceLogEntry(Speed(23)),
        25 → PerformanceLogEntry(Speed(23)),
        26 → PerformanceLogEntry(Speed(26))
      )
      val result1 = Autothrottler.optimize(24, logs, AutothrottleSettings(optimizationMinRange = 2, optimizationRangeRatio = 0.1))
      result1 shouldBe <=(26) //with limited sight, it only sees the nearby optimal pool size
      result1 shouldBe >(24)
      val result2 = Autothrottler.optimize(24, logs, AutothrottleSettings(optimizationMinRange = 2, optimizationRangeRatio = 0.3))
      result2 shouldBe <(24) //by looking further, it should see the further optimal pool size
      result2 shouldBe >=(20)
    }

    "ignore latency when weight is set to low" in {
      val logs = Map(
        20 → PerformanceLogEntry(Speed(22), Some(70.milliseconds)),
        22 → PerformanceLogEntry(Speed(22), Some(100.milliseconds)),
        24 → PerformanceLogEntry(Speed(23), Some(110.milliseconds))
      )
      val result = Autothrottler.optimize(22, logs, AutothrottleSettings(weightOfLatency = 0.1))
      result shouldBe >(22)
      result shouldBe <=(25)
    }

    "ignore further away sample data when optimizing" in new AutothrottleScope {
      val subject = autothrottlerRef(alwaysOptimizeSettings.copy(optimizationMinRange = 4, optimizationRangeRatio = 0.1))
      mockBusyHistory(
        subject,
        (10, 1999), //should be ignored
        (29, 2),
        (31, 2),
        (32, 2),
        (35, 3),
        (36, 3),
        (31, 3),
        (46, 4),
        (41, 8),
        (37, 6)
      )
      subject ! OptimizeOrExplore

      val scaleCmd = tProcessor.expectMsgType[ScaleTo]

      scaleCmd.reason.value shouldBe "optimizing"
      scaleCmd.numOfWorkers should be <= 41
      scaleCmd.numOfWorkers should be > 37
    }

    "downsize if hasn't maxed out for more than relevant period of hours" in new AutothrottleScope {
      val subject = autothrottlerRef(defaultSettings.copy(downsizeAfterUnderUtilization = 10.milliseconds))

      subject ! PartialUtilization(5)
      tProcessor.expectNoMsg(20.milliseconds)
      subject ! OptimizeOrExplore

      val scaleCmd = tProcessor.expectMsgType[ScaleTo]
      scaleCmd shouldBe ScaleTo(4, Some("downsizing"))
    }

    "stop itself if the QueueProcessor stops" in new ScopeWithActor() {
      val queue = TestProbe()
      val processor = system.actorOf(QueueProcessor.default(
        queue.ref,
        backend,
        ProcessingWorkerPoolSettings(),
        MetricsCollector(None)
      )(ResultChecker.expectType))

      watch(processor)
      val autothrottler = system.actorOf(Autothrottler.default(processor, AutothrottleSettings(), MetricsCollector(None)))
      watch(autothrottler)
      processor ! PoisonPill

      Set(expectMsgType[Terminated].actor, expectMsgType[Terminated].actor) shouldBe Set(processor, autothrottler)
    }

    "stop itself if the QueueProcessor is shutting down" in new ScopeWithActor() {
      val mc = MetricsCollector(None)
      val queue = TestProbe()
      val processor = system.actorOf(QueueProcessor.default(queue.ref, backend, ProcessingWorkerPoolSettings(), mc)(ResultChecker.expectType))
      //using 10 minutes to squelch its querying of the QueueProcessor, so that we can do it manually
      val a = system.actorOf(Autothrottler.default(processor, AutothrottleSettings(resizeInterval = 10.minutes), mc))
      watch(a)
      a ! PartialUtilization(5)
      processor ! Shutdown(None, 100.milliseconds)
      expectTerminated(a)
    }
  }
}

class AutothrottleScope(implicit system: ActorSystem)
  extends TestKit(system) with ImplicitSender {

  val metricsCollector: ActorRef = MetricsCollector(None) // To be overridden
  val defaultSettings: AutothrottleSettings = AutothrottleSettings(
    chanceOfScalingDownWhenFull = 0.3,
    resizeInterval = 1.hour, //manual action only
    explorationRatio = 0.5,
    downsizeRatio = 0.8,
    downsizeAfterUnderUtilization = 72.hours,
    optimizationMinRange = 6
  )

  val alwaysOptimizeSettings = defaultSettings.copy(explorationRatio = 0)
  val alwaysExploreSettings = defaultSettings.copy(explorationRatio = 1)

  val tProcessor = TestProbe()

  def autothrottlerRef(settings: AutothrottleSettings = defaultSettings) = {

    TestActorRef[Autothrottler](Autothrottler.default(
      tProcessor.ref, settings, metricsCollector
    ))
  }

  def mockBusyHistory(subject: ActorRef, ps: (PoolSize, Int)*) = {

    ps.zipWithIndex.foreach {
      case ((size, workDone), idx) ⇒
        val distance = ps.size - idx + 1

        subject ! Sample(
          workDone,
          start = distance.seconds.ago,
          end = (distance - 1).seconds.ago,
          poolSize = size,
          queueLength = QueueLength(14),
          None
        )
    }

  }

  lazy val as = autothrottlerRef()
}

object AutothrottleScope {
  import akka.actor.ActorDSL._

  def newWorker(busy: Boolean = true)(implicit system: ActorSystem) = actor(new Act {
    become {
      case _ ⇒ sender ! (if (busy) Working else Idle)
    }
  })
}
