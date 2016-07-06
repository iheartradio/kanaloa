package kanaloa.reactive.dispatcher.queue

import java.time.LocalDateTime

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.testkit._
import kanaloa.reactive.dispatcher.ApiProtocol.QueryStatus
import kanaloa.reactive.dispatcher.{ResultChecker, ScopeWithActor, SpecWithActorSystem}
import kanaloa.reactive.dispatcher.metrics.{Metric, MetricsCollector, NoOpMetricsCollector}
import kanaloa.reactive.dispatcher.queue.AutoScaling.{OptimizeOrExplore, PoolSize, UnderUtilizationStreak}
import kanaloa.reactive.dispatcher.queue.Queue.{QueueDispatchInfo, Retire}
import kanaloa.reactive.dispatcher.queue.QueueProcessor.{RunningStatus, ScaleTo, Shutdown}
import kanaloa.reactive.dispatcher.queue.Worker.{Idle, Working}
import org.scalatest.OptionValues
import org.scalatest.concurrent.Eventually
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

import scala.concurrent.duration._

class AutoScalingSpec extends SpecWithActorSystem with MockitoSugar with OptionValues with Eventually {
  import AutoScalingScope._

  "AutoScaling" should {
    "when no history" in new AutoScalingScope {
      as ! OptimizeOrExplore
      tQueue.expectMsgType[QueryStatus]
      tQueue.reply(MockQueueInfo(None))
      tProcessor.expectMsgType[QueryStatus]
    }

    "send metrics to metricsCollector" in new AutoScalingScope {
      override val metricsCollector: MetricsCollector = mock[MetricsCollector]
      val mc = metricsCollector

      as ! OptimizeOrExplore

      replyStatus(
        numOfBusyWorkers = 3,
        numOfIdleWorkers = 1
      )

      eventually {
        verify(mc).send(Metric.PoolSize(4))
        verify(mc).send(Metric.PoolUtilized(3))
        verifyNoMoreInteractions(mc)
      }
    }

    "record perfLog" in new AutoScalingScope {
      as ! OptimizeOrExplore
      tQueue.expectMsgType[QueryStatus]
      tQueue.reply(MockQueueInfo(Some(1.second)))
      tProcessor.expectMsgType[QueryStatus]
      tProcessor.reply(RunningStatus(Set(newWorker(), newWorker())))
      tProcessor.expectMsgType[ScaleTo]
      as.underlyingActor.perfLog should not be empty
    }

    "start an underutilizationStreak" in new AutoScalingScope {
      as ! OptimizeOrExplore
      replyStatus(numOfBusyWorkers = 3, numOfIdleWorkers = 1)
      tProcessor.expectNoMsg(15.millisecond)
      as.underlyingActor.underUtilizationStreak.get.highestUtilization === 3
    }

    "stop an underutilizationStreak" in new AutoScalingScope {
      as ! OptimizeOrExplore
      replyStatus(numOfBusyWorkers = 3, numOfIdleWorkers = 1)
      tProcessor.expectNoMsg(15.millisecond)
      as ! OptimizeOrExplore
      replyStatus(numOfBusyWorkers = 4, numOfIdleWorkers = 0)
      expectNoMsg(15.millisecond) //wait
      as.underlyingActor.underUtilizationStreak shouldBe empty
    }

    "update an underutilizationStreak" in new AutoScalingScope {
      as ! OptimizeOrExplore
      replyStatus(numOfBusyWorkers = 3, numOfIdleWorkers = 1)
      tProcessor.expectNoMsg(50.millisecond)
      val start = as.underlyingActor.underUtilizationStreak.get.start
      as ! OptimizeOrExplore
      replyStatus(numOfBusyWorkers = 5, numOfIdleWorkers = 1)

      tProcessor.expectNoMsg(15.millisecond)
      as.underlyingActor.underUtilizationStreak.get.start === start
      as.underlyingActor.underUtilizationStreak.get.highestUtilization === 5
    }

    "explore when currently maxed out and exploration rate is 1" in new AutoScalingScope {
      val subject = autoScalingRef(explorationRatio = 1)
      subject ! OptimizeOrExplore
      replyStatus(numOfBusyWorkers = 3, numOfIdleWorkers = 0)

      val scaleCmd = tProcessor.expectMsgType[ScaleTo]

      scaleCmd.reason.value shouldBe "exploring"
    }

    "does not optimize when not currently maxed" in new AutoScalingScope {
      val subject = autoScalingRef(explorationRatio = 1)
      subject ! OptimizeOrExplore
      replyStatus(numOfBusyWorkers = 3, numOfIdleWorkers = 0)
      tProcessor.expectMsgType[ScaleTo]
      subject ! OptimizeOrExplore
      replyStatus(numOfBusyWorkers = 3, numOfIdleWorkers = 1)
      tProcessor.expectNoMsg(30.millisecond)
    }

    "optimize towards the faster size when currently maxed out and exploration rate is 0" in new AutoScalingScope {
      val subject = autoScalingRef(explorationRatio = 0)
      mockBusyHistory(
        subject,
        (30, 32.milliseconds),
        (30, 30.milliseconds),
        (40, 20.milliseconds),
        (40, 23.milliseconds),
        (35, 25.milliseconds)
      )
      subject ! OptimizeOrExplore
      replyStatus(numOfBusyWorkers = 32, numOfIdleWorkers = 0, dispatchDuration = 43.milliseconds)
      val scaleCmd = tProcessor.expectMsgType[ScaleTo]

      scaleCmd.reason.value shouldBe "optimizing"
      scaleCmd.numOfWorkers should be > 35
      scaleCmd.numOfWorkers should be < 40
    }

    "ignore further away sample data when optmizing" in new AutoScalingScope {
      val subject = autoScalingRef(explorationRatio = 0)
      mockBusyHistory(
        subject,
        (10, 1.milliseconds), //should be ignored
        (29, 32.milliseconds),
        (31, 32.milliseconds),
        (32, 32.milliseconds),
        (35, 32.milliseconds),
        (36, 32.milliseconds),
        (31, 30.milliseconds),
        (43, 20.milliseconds),
        (41, 23.milliseconds),
        (37, 25.milliseconds)
      )
      subject ! OptimizeOrExplore
      replyStatus(numOfBusyWorkers = 37, numOfIdleWorkers = 0, dispatchDuration = 28.milliseconds)
      val scaleCmd = tProcessor.expectMsgType[ScaleTo]

      scaleCmd.reason.value shouldBe "optimizing"
      scaleCmd.numOfWorkers should be > 35
      scaleCmd.numOfWorkers should be < 43
    }

    "do nothing if not enough history in general " in new AutoScalingScope {
      val subject = autoScalingRef(explorationRatio = 1)
      subject ! OptimizeOrExplore
      replyStatus(numOfBusyWorkers = 3, numOfIdleWorkers = 1)

      tProcessor.expectNoMsg(20.milliseconds)
      subject ! OptimizeOrExplore
      replyStatus(numOfBusyWorkers = 3, numOfIdleWorkers = 1)

      tProcessor.expectNoMsg(50.milliseconds)
    }

    "downsize if hasn't maxed out for more than relevant period of hours" in new AutoScalingScope {
      val moreThan72HoursAgo = LocalDateTime.now.minusHours(73)
      as.underlyingActor.underUtilizationStreak = Some(UnderUtilizationStreak(moreThan72HoursAgo, 40))

      as ! OptimizeOrExplore
      replyStatus(numOfBusyWorkers = 34, numOfIdleWorkers = 16)
      val scaleCmd = tProcessor.expectMsgType[ScaleTo]
      scaleCmd shouldBe ScaleTo(32, Some("downsizing"))
    }

    "do not thing if hasn't maxed out for shorter than relevant period of hours" in new AutoScalingScope {
      val lessThan72HoursAgo = LocalDateTime.now.minusHours(71)
      as.underlyingActor.underUtilizationStreak = Some(UnderUtilizationStreak(lessThan72HoursAgo, 40))

      as ! OptimizeOrExplore
      replyStatus(numOfBusyWorkers = 34, numOfIdleWorkers = 16)
      tProcessor.expectNoMsg(50.millis)
    }

    "stop itself if the Queue stops" in new AutoScalingScope {
      val queue = system.actorOf(Queue.default())
      watch(queue)
      val processor = TestProbe()
      val a = system.actorOf(AutoScaling.default(queue, processor.ref, AutoScalingSettings()))
      watch(a)
      queue ! Retire(1.millisecond)
      expectTerminated(queue)
      expectTerminated(a)
    }

    "stop itself if the QueueProcessor stops" in new ScopeWithActor() {
      val queue = TestProbe()
      val processor = system.actorOf(QueueProcessor.default(queue.ref, backend, ProcessingWorkerPoolSettings())(ResultChecker.simple))
      watch(processor)
      val a = system.actorOf(AutoScaling.default(queue.ref, processor, AutoScalingSettings()))
      watch(a)
      processor ! PoisonPill
      expectTerminated(processor)
      expectTerminated(a)
    }

    "stop itself if the QueueProcessor is shutting down" in new ScopeWithActor() {
      val queue = TestProbe()
      val processor = system.actorOf(QueueProcessor.default(queue.ref, backend, ProcessingWorkerPoolSettings())(ResultChecker.simple))
      watch(processor)
      //using 10 minutes to squelch its querying of the QueueProcessor, so that we can do it manually
      val a = system.actorOf(AutoScaling.default(queue.ref, processor, AutoScalingSettings(actionFrequency = 10.minutes)))
      watch(a)
      //let this thing take its sweet time shutting down
      processor ! Shutdown(None, 100.milliseconds, false)
      a ! OptimizeOrExplore
      a ! kanaloa.reactive.dispatcher.queue.Queue.QueueStatus()
      expectTerminated(a)
      expectTerminated(processor)
    }
  }
}

class AutoScalingScope(implicit system: ActorSystem)
  extends TestKit(system) with ImplicitSender {

  val metricsCollector: MetricsCollector = NoOpMetricsCollector // To be overridden

  val tQueue = TestProbe()
  val tProcessor = TestProbe()
  import AutoScalingScope._

  def autoScalingRef(explorationRatio: Double = 0.5) =
    TestActorRef[AutoScaling](AutoScaling.default(tQueue.ref, tProcessor.ref,
      AutoScalingSettings(
        chanceOfScalingDownWhenFull = 0.3,
        actionFrequency = 1.hour, //manual action only
        explorationRatio = explorationRatio,
        downsizeRatio = 0.8,
        numOfAdjacentSizesToConsiderDuringOptimization = 6
      ), metricsCollector))

  def replyStatus(numOfBusyWorkers: Int, dispatchDuration: Duration = 5.milliseconds, numOfIdleWorkers: Int = 0): Unit = {
    tQueue.expectMsgType[QueryStatus]
    val fullyUtilized = numOfIdleWorkers == 0
    tQueue.reply(MockQueueInfo(if (fullyUtilized) Some(dispatchDuration) else None))

    tProcessor.expectMsgType[QueryStatus]
    val workers = (1 to numOfBusyWorkers).map(_ ⇒ newWorker(true)) ++
      (1 to numOfIdleWorkers).map(_ ⇒ newWorker(false))
    tProcessor.reply(RunningStatus(workers.toSet))

  }

  def mockBusyHistory(subject: ActorRef, ps: (PoolSize, Duration)*) = {
    ps.foreach {
      case (size, duration) ⇒
        subject ! OptimizeOrExplore
        replyStatus(numOfBusyWorkers = size, dispatchDuration = duration)
        tProcessor.expectMsgType[ScaleTo]
    }

  }

  lazy val as = autoScalingRef()
}

object AutoScalingScope {
  import akka.actor.ActorDSL._
  case class MockQueueInfo(avgDispatchDurationLowerBoundWhenFullyUtilized: Option[Duration]) extends QueueDispatchInfo

  def newWorker(busy: Boolean = true)(implicit system: ActorSystem) = actor(new Act {
    become {
      case _ ⇒ sender ! (if (busy) Working else Idle)
    }
  })
}
