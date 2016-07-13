package kanaloa.reactive.dispatcher.queue

import java.time.LocalDateTime

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.testkit._
import kanaloa.reactive.dispatcher.ApiProtocol.QueryStatus
import kanaloa.reactive.dispatcher.metrics.MetricsCollector.{PartialUtilization, Sample}
import kanaloa.reactive.dispatcher.{ResultChecker, ScopeWithActor, SpecWithActorSystem}
import kanaloa.reactive.dispatcher.metrics.{MetricsCollector, Metric}
import kanaloa.reactive.dispatcher.queue.AutoScaling.{AutoScalingStatus, OptimizeOrExplore, PoolSize}
import kanaloa.reactive.dispatcher.queue.Queue.{QueueDispatchInfo, Retire}
import kanaloa.reactive.dispatcher.queue.QueueProcessor.{RunningStatus, ScaleTo, Shutdown}
import kanaloa.reactive.dispatcher.queue.Worker.{Idle, Working}
import org.scalatest.OptionValues
import org.scalatest.concurrent.Eventually
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import kanaloa.reactive.dispatcher.DurationFunctions._
import scala.concurrent.duration._

class AutoScalingSpec extends SpecWithActorSystem with MockitoSugar with OptionValues with Eventually {
  import AutoScalingScope._

  "AutoScaling" should {
    "when no history" in new AutoScalingScope {
      as ! OptimizeOrExplore
      tProcessor.expectNoMsg(50.milliseconds)
    }

    "record perfLog" in new AutoScalingScope {
      as ! Sample(3, 2.second.ago, 1.second.ago, 30)
      as ! QueryStatus()
      val status = expectMsgType[AutoScalingStatus]
      status.poolSize should contain(30)
      status.performanceLog.keys should contain(30)
    }

    "update poolsize" in new AutoScalingScope {
      as ! Sample(3, 2.second.ago, 1.second.ago, 30)
      as ! Sample(3, 2.second.ago, 1.second.ago, 33)
      as ! Sample(3, 2.second.ago, 1.second.ago, 35)
      as ! QueryStatus()
      val status = expectMsgType[AutoScalingStatus]
      status.poolSize should contain(35)
      status.performanceLog.keys should contain(33)
    }

    "start an underutilizationStreak" in new AutoScalingScope {
      as ! PartialUtilization(3)
      as ! QueryStatus()
      val status = expectMsgType[AutoScalingStatus]
      status.partialUtilization should contain(3)
      status.partialUtilizationStart should not be (empty)
    }

    "stop an underutilizationStreak" in new AutoScalingScope {
      as ! PartialUtilization(3)
      as ! Sample(3, 2.second.ago, 1.second.ago, 30)

      as ! QueryStatus()
      val status = expectMsgType[AutoScalingStatus]
      status.partialUtilization should be(empty)
      status.partialUtilizationStart should be(empty)
    }

    "update an underutilizationStreak to the highest utilization" in new AutoScalingScope {
      as ! PartialUtilization(3)
      as ! QueryStatus()

      val status1 = expectMsgType[AutoScalingStatus]

      as ! PartialUtilization(5)
      as ! QueryStatus()

      val status2 = expectMsgType[AutoScalingStatus]

      as ! PartialUtilization(4)
      as ! QueryStatus()

      val status3 = expectMsgType[AutoScalingStatus]

      status1.partialUtilizationStart should not be (empty)
      status1.partialUtilizationStart should be(status3.partialUtilizationStart)
      status3.partialUtilization should contain(5)
    }

    "explore when currently maxed out and exploration rate is 1" in new AutoScalingScope {
      val subject = autoScalingRef(alwaysExploreSettings)
      subject ! Sample(3, 2.second.ago, 1.second.ago, 30)

      subject ! OptimizeOrExplore

      val scaleCmd = tProcessor.expectMsgType[ScaleTo]

      scaleCmd.reason.value shouldBe "exploring"
    }

    "does not optimize when not currently maxed" in new AutoScalingScope {
      val subject = autoScalingRef()
      subject ! Sample(3, 2.second.ago, 1.second.ago, 30)

      subject ! OptimizeOrExplore
      tProcessor.expectMsgType[ScaleTo]

      subject ! PartialUtilization(4)

      subject ! OptimizeOrExplore

      tProcessor.expectNoMsg(30.millisecond)
    }

    "optimize towards the faster size when currently maxed out and exploration rate is 0" in new AutoScalingScope {
      val subject = autoScalingRef(alwaysOptimizeSettings)
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

    "ignore further away sample data when optmizing" in new AutoScalingScope {
      val subject = autoScalingRef(alwaysOptimizeSettings)
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
      scaleCmd.numOfWorkers should be > 35
      scaleCmd.numOfWorkers should be < 44
    }

    "downsize if hasn't maxed out for more than relevant period of hours" in new AutoScalingScope {
      val subject = autoScalingRef(defaultSettings.copy(downsizeAfterUnderUtilization = 10.milliseconds))

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
      )(ResultChecker.simple))

      watch(processor)
      val a = system.actorOf(AutoScaling.default(processor, AutoScalingSettings(), MetricsCollector(None)))
      watch(a)
      processor ! PoisonPill
      expectTerminated(processor)
      expectTerminated(a)
    }

    "stop itself if the QueueProcessor is shutting down" in new ScopeWithActor() {
      val mc = MetricsCollector(None)
      val queue = TestProbe()
      val processor = system.actorOf(QueueProcessor.default(queue.ref, backend, ProcessingWorkerPoolSettings(), mc)(ResultChecker.simple))
      //using 10 minutes to squelch its querying of the QueueProcessor, so that we can do it manually
      val a = system.actorOf(AutoScaling.default(processor, AutoScalingSettings(actionInterval = 10.minutes), mc))
      watch(a)
      a ! PartialUtilization(5)
      processor ! Shutdown(None, 100.milliseconds, false)
      expectTerminated(a)
    }
  }
}

class AutoScalingScope(implicit system: ActorSystem)
  extends TestKit(system) with ImplicitSender {

  val metricsCollector: ActorRef = MetricsCollector(None) // To be overridden
  val defaultSettings: AutoScalingSettings = AutoScalingSettings(
    chanceOfScalingDownWhenFull = 0.3,
    actionInterval = 1.hour, //manual action only
    explorationRatio = 0.5,
    downsizeRatio = 0.8,
    downsizeAfterUnderUtilization = 72.hours,
    numOfAdjacentSizesToConsiderDuringOptimization = 6
  )

  val alwaysOptimizeSettings = defaultSettings.copy(explorationRatio = 0)
  val alwaysExploreSettings = defaultSettings.copy(explorationRatio = 1)

  val tProcessor = TestProbe()

  def autoScalingRef(settings: AutoScalingSettings = defaultSettings) = {

    TestActorRef[AutoScaling](AutoScaling.default(
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
          size
        )
    }

  }

  lazy val as = autoScalingRef()
}

object AutoScalingScope {
  import akka.actor.ActorDSL._
  case class MockQueueInfo(avgDequeueDurationLowerBoundWhenFullyUtilized: Option[Duration]) extends QueueDispatchInfo

  def newWorker(busy: Boolean = true)(implicit system: ActorSystem) = actor(new Act {
    become {
      case _ ⇒ sender ! (if (busy) Working else Idle)
    }
  })
}
