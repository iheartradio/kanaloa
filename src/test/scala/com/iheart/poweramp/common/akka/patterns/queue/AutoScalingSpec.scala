package com.iheart.poweramp.common.akka.patterns.queue

import java.time.LocalDateTime

import akka.actor.Actor.Receive
import akka.actor.{Props, ActorRef, Actor, ActorSystem}
import akka.testkit.TestActor.AutoPilot
import akka.testkit._
import com.iheart.poweramp.common.akka.SpecWithActorSystem
import com.iheart.poweramp.common.akka.patterns.CommonProtocol.QueryStatus
import com.iheart.poweramp.common.akka.patterns.queue.AutoScaling.{PoolSize, PerformanceLogEntry, OptimizeOrExplore}
import com.iheart.poweramp.common.akka.patterns.queue.Queue.QueueDispatchInfo
import com.iheart.poweramp.common.akka.patterns.queue.QueueProcessor.{ScaleTo, RunningStatus}
import com.iheart.poweramp.common.akka.patterns.queue.Worker.{Idle, Working}
import org.specs2.specification.Scope

import scala.concurrent.duration._

class AutoScalingSpec extends SpecWithActorSystem {
  "when no history" in new AutoScalingScope {
    as ! OptimizeOrExplore
    tQueue.expectMsgType[QueryStatus]
    tQueue.reply(MockQueueInfo(None))
    tProcessor.expectNoMsg(40.milliseconds)
  }

  "record perfLog" in new AutoScalingScope {
    as ! OptimizeOrExplore
    tQueue.expectMsgType[QueryStatus]
    tQueue.reply(MockQueueInfo(Some(1.second)))
    tProcessor.expectMsgType[QueryStatus]
    tProcessor.reply(RunningStatus(Set(newWorker().ref, newWorker().ref)))
    tProcessor.expectMsgType[ScaleTo]
    as.underlyingActor.perfLog must not(beEmpty)
  }

  "explore when currently maxed out and exploration rate is 1" in new AutoScalingScope {
    val subject = autoScalingRef(explorationRatio = 1)
    subject ! OptimizeOrExplore
    replyStatus(numOfBusyWorkers = 3, numOfIdleWorkers = 0)

    val scaleCmd = tProcessor.expectMsgType[ScaleTo]

    scaleCmd.reason must beSome("exploring")
  }


  "optimize when not currently maxed" in new AutoScalingScope {
    val subject = autoScalingRef(explorationRatio = 1)
    subject ! OptimizeOrExplore
    replyStatus(numOfBusyWorkers = 3, numOfIdleWorkers = 0)
    tProcessor.expectMsgType[ScaleTo]
    subject ! OptimizeOrExplore
    replyStatus(numOfBusyWorkers = 3, numOfIdleWorkers = 1)
    val scaleCmd = tProcessor.expectMsgType[ScaleTo]

    scaleCmd.reason must beSome("optimizing")
  }


  "optimize towards the faster size when currently maxed out and exploration rate is 0" in new AutoScalingScope {
    val subject = autoScalingRef(explorationRatio = 0)
    mockBusyHistory(subject,
                            (30, 32.milliseconds),
                            (30, 30.milliseconds),
                            (40, 20.milliseconds),
                            (40, 23.milliseconds),
                            (35, 25.milliseconds))
    subject ! OptimizeOrExplore
    replyStatus(numOfBusyWorkers = 32, numOfIdleWorkers = 0, dispatchDuration = 43.milliseconds)
    val scaleCmd = tProcessor.expectMsgType[ScaleTo]

    scaleCmd.reason must beSome("optimizing")
    scaleCmd.numOfWorkers must be_>(35)
    scaleCmd.numOfWorkers must be_<(40)
  }

  "ignore further away sample data when optmizing" in new AutoScalingScope {
    val subject = autoScalingRef(explorationRatio = 0)
    mockBusyHistory(subject,
                            (10, 1.milliseconds), //should be ignored
                            (29, 32.milliseconds),
                            (31, 32.milliseconds),
                            (32, 32.milliseconds),
                            (35, 32.milliseconds),
                            (36, 32.milliseconds),
                            (31, 30.milliseconds),
                            (43, 20.milliseconds),
                            (41, 23.milliseconds),
                            (37, 25.milliseconds))
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

    subject ! OptimizeOrExplore
    replyStatus(numOfBusyWorkers = 3, numOfIdleWorkers = 1)

    tProcessor.expectNoMsg(50.milliseconds)
  }

  "downsize if hasn't maxed out for more than relevant period of hours" in new AutoScalingScope {
    val almost72HoursAgo =  LocalDateTime.now.minusHours(72).plusMinutes(30)
    as.underlyingActor.perfLog = Vector(PerformanceLogEntry(50, 1.seconds, 12, almost72HoursAgo),
      PerformanceLogEntry(50, 1.seconds, 40, LocalDateTime.now.minusHours(34)),
      PerformanceLogEntry(50, 1.seconds, 24, LocalDateTime.now.minusHours(24))
    )

    as ! OptimizeOrExplore
    replyStatus(numOfBusyWorkers = 34, numOfIdleWorkers = 16)
    val scaleCmd = tProcessor.expectMsgType[ScaleTo]
    scaleCmd === ScaleTo(44, Some("downsizing"))
  }
}

class AutoScalingScope(implicit system: ActorSystem) extends TestKit(system) with ImplicitSender with Scope {
  val tQueue = TestProbe()
  val tProcessor = TestProbe()

  def newWorker(busy: Boolean = true) = {
    val w = TestProbe()
    w.setAutoPilot(new AutoPilot {
      def run(sender: ActorRef, msg: Any): AutoPilot = {
        sender ! (if(busy) Working else Idle)
        TestActor.KeepRunning
      }
    })
    w
  }


  case class MockQueueInfo(avgDispatchDurationLowerBound: Option[Duration]) extends QueueDispatchInfo

  case class TestAutoScaling(override val explorationRatio: Double) extends AutoScaling {
    val queue = tQueue.ref
    val processor = tProcessor.ref
    override def chanceOfScalingDownWhenFull = 0.3

    override def actionFrequency: FiniteDuration = 1.hour //manual action
  }

  def autoScalingRef(explorationRatio: Double = 0.5) =
    TestActorRef[TestAutoScaling](Props(TestAutoScaling(explorationRatio)))

  def replyStatus(numOfBusyWorkers: Int, dispatchDuration: Duration = 5.milliseconds, numOfIdleWorkers: Int = 0): Unit = {
    tQueue.expectMsgType[QueryStatus]
    tQueue.reply(MockQueueInfo(Some(dispatchDuration)))
    tProcessor.expectMsgType[QueryStatus]
    val workers = (1 to numOfBusyWorkers).map(_ => newWorker(true).ref) ++
      (1 to numOfIdleWorkers).map(_ => newWorker(false).ref)
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

  val as = autoScalingRef()
}


