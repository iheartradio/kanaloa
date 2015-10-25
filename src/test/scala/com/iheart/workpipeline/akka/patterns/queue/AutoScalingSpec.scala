package com.iheart.workpipeline.akka.patterns.queue

import java.time.LocalDateTime

import akka.actor.{ ActorRef, ActorSystem }
import akka.testkit._
import com.iheart.workpipeline.akka.{ SpecWithActorSystem, patterns }
import AutoScaling.{ UnderUtilizationStreak, PoolSize, OptimizeOrExplore }
import QueueProcessor.RunningStatus
import com.iheart.workpipeline.akka.patterns.CommonProtocol.QueryStatus
import com.iheart.workpipeline.akka.patterns.queue.Queue.QueueDispatchInfo
import com.iheart.workpipeline.akka.patterns.queue.QueueProcessor.ScaleTo
import com.iheart.workpipeline.akka.patterns.queue.Worker.{ Idle, Working }
import com.iheart.workpipeline.metrics.{ Metric, MetricsCollector, NoOpMetricsCollector }
import org.specs2.specification.Scope
import org.specs2.mock.Mockito

import scala.concurrent.duration._

class AutoScalingSpec extends SpecWithActorSystem with Mockito {
  "when no history" in new AutoScalingScope {
    as ! OptimizeOrExplore
    tQueue.expectMsgType[QueryStatus]
    tQueue.reply(MockQueueInfo(None))
    tProcessor.expectNoMsg(40.milliseconds)
  }

  "send metrics to metricsCollector" in new AutoScalingScope {
    override val metricsCollector: MetricsCollector = mock[MetricsCollector]
    val mc = metricsCollector

    as ! OptimizeOrExplore

    replyStatus(
      numOfBusyWorkers = 3,
      numOfIdleWorkers = 1
    )

    there was after(50.milliseconds).
      one(mc).send(Metric.PoolSize(4)) andThen
      one(mc).send(Metric.PoolUtilized(3))
  }

  "record perfLog" in new AutoScalingScope {
    as ! OptimizeOrExplore
    tQueue.expectMsgType[QueryStatus]
    tQueue.reply(MockQueueInfo(Some(1.second)))
    tProcessor.expectMsgType[QueryStatus]
    tProcessor.reply(RunningStatus(Set(newWorker(), newWorker())))
    tProcessor.expectMsgType[ScaleTo]
    as.underlyingActor.perfLog must not(beEmpty)
  }

  "start an underutilizationStreak" in new AutoScalingScope {
    as ! OptimizeOrExplore
    replyStatus(numOfBusyWorkers = 3, numOfIdleWorkers = 1)
    tProcessor.expectNoMsg(5.millisecond)
    as.underlyingActor.underUtilizationStreak.get.highestUtilization === 3
  }

  "stop an underutilizationStreak" in new AutoScalingScope {
    as ! OptimizeOrExplore
    replyStatus(numOfBusyWorkers = 3, numOfIdleWorkers = 1)
    tProcessor.expectNoMsg(5.millisecond)
    as ! OptimizeOrExplore
    replyStatus(numOfBusyWorkers = 4, numOfIdleWorkers = 0)
    expectNoMsg(5.millisecond) //wait
    as.underlyingActor.underUtilizationStreak must beEmpty
  }

  "update an underutilizationStreak" in new AutoScalingScope {
    as ! OptimizeOrExplore
    replyStatus(numOfBusyWorkers = 3, numOfIdleWorkers = 1)
    tProcessor.expectNoMsg(50.millisecond)
    val start = as.underlyingActor.underUtilizationStreak.get.start
    as ! OptimizeOrExplore
    replyStatus(numOfBusyWorkers = 5, numOfIdleWorkers = 1)

    tProcessor.expectNoMsg(5.millisecond)
    as.underlyingActor.underUtilizationStreak.get.start === start
    as.underlyingActor.underUtilizationStreak.get.highestUtilization === 5
  }

  "explore when currently maxed out and exploration rate is 1" in new AutoScalingScope {
    val subject = autoScalingRef(explorationRatio = 1)
    subject ! OptimizeOrExplore
    replyStatus(numOfBusyWorkers = 3, numOfIdleWorkers = 0)

    val scaleCmd = tProcessor.expectMsgType[ScaleTo]

    scaleCmd.reason must beSome("exploring")
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

    scaleCmd.reason must beSome("optimizing")
    scaleCmd.numOfWorkers must be_>(35)
    scaleCmd.numOfWorkers must be_<(40)
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

    scaleCmd.reason must beSome("optimizing")
    scaleCmd.numOfWorkers must be_>(35)
    scaleCmd.numOfWorkers must be_<(43)
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
    scaleCmd === ScaleTo(32, Some("downsizing"))
  }

  "do not thing if hasn't maxed out for shorter than relevant period of hours" in new AutoScalingScope {
    val lessThan72HoursAgo = LocalDateTime.now.minusHours(71)
    as.underlyingActor.underUtilizationStreak = Some(UnderUtilizationStreak(lessThan72HoursAgo, 40))

    as ! OptimizeOrExplore
    replyStatus(numOfBusyWorkers = 34, numOfIdleWorkers = 16)
    tProcessor.expectNoMsg(50.millis)
  }

  "do not go beyond upperBound when optimizing" in new AutoScalingScope {
    val subject = autoScalingRef(explorationRatio = 0)
    subject.underlyingActor.perfLog = Map(350 -> 2.nanosecond, 400 -> 1.nanosecond)
    subject ! OptimizeOrExplore
    replyStatus(numOfBusyWorkers = 295, dispatchDuration = 10.milliseconds)

    val scaleCmd = tProcessor.expectMsgType[ScaleTo]
    scaleCmd === ScaleTo(300, Some("optimizing"))

  }
}

class AutoScalingScope(implicit system: ActorSystem)
    extends TestKit(system) with ImplicitSender with Scope {

  val metricsCollector: MetricsCollector = NoOpMetricsCollector // To be overridden

  val tQueue = TestProbe()
  val tProcessor = TestProbe()
  import akka.actor.ActorDSL._

  def newWorker(busy: Boolean = true) = actor(new Act {
    become {
      case _ => sender ! (if (busy) Working else Idle)
    }
  })

  case class MockQueueInfo(avgDispatchDurationLowerBound: Option[Duration]) extends QueueDispatchInfo

  def autoScalingRef(explorationRatio: Double = 0.5) =
    TestActorRef[AutoScaling](AutoScaling.default(tQueue.ref, tProcessor.ref,
      AutoScalingSettings(
        chanceOfScalingDownWhenFull = 0.3,
        actionFrequency = 1.hour, //manual action only
        explorationRatio = explorationRatio,
        bufferRatio = 0.8,
        numOfAdjacentSizesToConsiderDuringOptimization = 6,
        upperBound = 300
      ), metricsCollector))

  def replyStatus(numOfBusyWorkers: Int, dispatchDuration: Duration = 5.milliseconds, numOfIdleWorkers: Int = 0): Unit = {
    tQueue.expectMsgType[QueryStatus]
    tQueue.reply(MockQueueInfo(Some(dispatchDuration)))
    tProcessor.expectMsgType[QueryStatus]
    val workers = (1 to numOfBusyWorkers).map(_ => newWorker(true)) ++
      (1 to numOfIdleWorkers).map(_ => newWorker(false))
    tProcessor.reply(RunningStatus(workers.toSet))
  }

  def mockBusyHistory(subject: ActorRef, ps: (PoolSize, Duration)*) = {
    ps.foreach {
      case (size, duration) =>
        subject ! OptimizeOrExplore
        replyStatus(numOfBusyWorkers = size, dispatchDuration = duration)
        tProcessor.expectMsgType[ScaleTo]
    }

  }

  lazy val as = autoScalingRef()
}

