package kanaloa.dispatcher

import java.time.{LocalDateTime ⇒ Time}

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.TestProbe
import kanaloa.dispatcher.PerformanceSampler._
import kanaloa.dispatcher.Types.QueueLength
import kanaloa.dispatcher.metrics.Metric._
import kanaloa.dispatcher.metrics.{MetricsCollector, Reporter}
import kanaloa.dispatcher.queue.Queue
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually
import org.scalatest.mock.MockitoSugar

import scala.concurrent.duration._

class PerformanceSamplerSpec extends SpecWithActorSystem with MockitoSugar with Eventually {
  val waitDuration = 30.milliseconds
  val startingPoolSize: Int = 10

  def initPerformanceSampler(
    minSampleDurationRatio: Double         = 0,
    sampleInterval:         FiniteDuration = 30.seconds //relies on manual AddSample signal in tests
  )(implicit system: ActorSystem): (ActorRef, TestProbe) = {
    val ps = system.actorOf(MetricsCollector.props(None, PerformanceSamplerSettings(
      sampleInterval = sampleInterval,
      minSampleDurationRatio = minSampleDurationRatio
    )))
    ps ! fullyUtilizedStatus //set it in the busy mode
    ps ! PoolSize(startingPoolSize)
    val subscriberProbe = TestProbe()
    ps ! Subscribe(subscriberProbe.ref)
    (ps, subscriberProbe)
  }
  val partialUtilizedStatus: Queue.Status = Queue.Status(1, QueueLength(0), false)
  val fullyUtilizedStatus: Queue.Status = Queue.Status(0, QueueLength(2), true)

  "PerformanceSampler" should {
    "send Samples periodically" in {
      val (ps, subscriberProbe) = initPerformanceSampler(sampleInterval = 100.milliseconds)
      ps ! WorkCompleted(1.millisecond)
      ps ! WorkCompleted(1.millisecond)

      val sample1 = subscriberProbe.expectMsgType[Sample]
      sample1.workDone shouldBe 2

      ps ! WorkCompleted(1.millisecond)

      val sample2 = subscriberProbe.expectMsgType[Sample]
      sample2.workDone shouldBe 1

      sample2.start.isAfter(sample1.start) shouldBe true

    }

    "collects avgProcess time" in {
      val (ps, subscriberProbe) = initPerformanceSampler(sampleInterval = 100.milliseconds)
      ps ! WorkCompleted(1.millisecond)
      ps ! WorkCompleted(5.millisecond)

      val sample1 = subscriberProbe.expectMsgType[Sample]
      sample1.avgProcessTime should contain(3.milliseconds)

      ps ! WorkCompleted(5.millisecond)
      ps ! WorkCompleted(9.millisecond)
      ps ! WorkCompleted(4.millisecond)

      val sample2 = subscriberProbe.expectMsgType[Sample]
      sample2.avgProcessTime should contain(6.milliseconds)

    }

    "ignore metrics when pool isn't fully occupied" in {
      val (ps, subscriberProbe) = initPerformanceSampler()
      ps ! partialUtilizedStatus

      subscriberProbe.expectMsgType[Sample] //last sample when fully utilized
      subscriberProbe.expectMsgType[PartialUtilization].numOfBusyWorkers shouldBe 9

      ps ! WorkCompleted(1.millisecond)
      ps ! WorkCompleted(1.millisecond)
      ps ! AddSample
      subscriberProbe.expectNoMsg(waitDuration)

    }

    "sends sample without work" in {
      val (ps, subscriberProbe) = initPerformanceSampler()

      ps ! AddSample

      subscriberProbe.expectMsgType[Sample].workDone shouldBe 0

    }

    "continually sends sample without work without reseting start" in {
      val (ps, subscriberProbe) = initPerformanceSampler()

      ps ! AddSample

      val sample1 = subscriberProbe.expectMsgType[Sample]

      Thread.sleep(30) //add a distance between first and second sample
      ps ! Queue.Status(0, QueueLength(4), true)
      ps ! AddSample
      val sample2 = subscriberProbe.expectMsgType[Sample]
      sample2.end.isAfter(sample1.end) shouldBe true
      sample1.start shouldBe sample2.start
      sample2.queueLength shouldBe QueueLength(4)

    }

    "ignore Work timeout but include failed Work " in {
      val (ps, subscriberProbe) = initPerformanceSampler()
      ps ! WorkTimedOut
      ps ! AddSample
      subscriberProbe.expectMsgType[Sample].workDone shouldBe 0

      ps ! WorkFailed
      ps ! AddSample

      subscriberProbe.expectMsgType[Sample].workDone shouldBe 1
    }

    "resume to collect metrics once pool becomes busy again, but doesn't count old work" in {
      val (ps, subscriberProbe) = initPerformanceSampler()
      ps ! partialUtilizedStatus
      subscriberProbe.expectMsgType[Sample] //last sample when fully utilized
      subscriberProbe.expectMsgType[PartialUtilization]

      ps ! WorkCompleted(1.millisecond)
      ps ! WorkCompleted(1.millisecond)

      ps ! fullyUtilizedStatus

      ps ! WorkCompleted(1.millisecond)

      ps ! AddSample
      subscriberProbe.expectMsgType[Sample].workDone shouldBe 1

    }

    "reset counter when pool size changed" in {
      val (ps, subscriberProbe) = initPerformanceSampler()

      ps ! WorkCompleted(1.millisecond)
      ps ! WorkCompleted(1.millisecond)

      ps ! AddSample

      subscriberProbe.expectMsgType[Sample].workDone shouldBe 2

      ps ! PoolSize(12)
      subscriberProbe.expectMsgType[Sample].workDone shouldBe 0

      ps ! WorkCompleted(1.millisecond)

      ps ! AddSample

      val sample = subscriberProbe.expectMsgType[Sample]
      sample.workDone shouldBe 1
      sample.poolSize shouldBe 12

    }

    "remember queue length when pool size changed" in {
      val (ps, subscriberProbe) = initPerformanceSampler()

      ps ! Queue.Status(0, QueueLength(11), true)
      ps ! AddSample

      subscriberProbe.expectMsgType[Sample].queueLength.value shouldBe 11
      ps ! PoolSize(12)
      subscriberProbe.expectMsgType[Sample]

      ps ! AddSample

      subscriberProbe.expectMsgType[Sample].queueLength.value shouldBe 11
    }

    "register pool size when resting" in {
      val (ps, subscriberProbe) = initPerformanceSampler()

      ps ! partialUtilizedStatus
      subscriberProbe.expectMsgType[Sample]
      subscriberProbe.expectMsgType[PartialUtilization]

      ps ! PoolSize(15)
      ps ! fullyUtilizedStatus
      ps ! WorkCompleted(1.millisecond)
      ps ! AddSample
      subscriberProbe.expectMsgType[Sample].poolSize shouldBe 15

    }

    "register queue length" in {
      val (ps, subscriberProbe) = initPerformanceSampler()

      ps ! Queue.Status(0, QueueLength(21), true)
      ps ! WorkCompleted(1.millisecond)
      ps ! AddSample

      subscriberProbe.expectMsgType[Sample].queueLength shouldBe QueueLength(21)

    }

    "continue counting when sample duration not long enough" in {
      val (ps, subscriberProbe) = initPerformanceSampler(0.99, waitDuration)
      ps ! WorkCompleted(1.millisecond)
      ps ! AddSample
      subscriberProbe.expectNoMsg(waitDuration / 3)

      ps ! WorkCompleted(1.millisecond)

      subscriberProbe.expectMsgType[Sample].workDone shouldBe 2
    }

    "reset counting when pool size changed" in {
      val (ps, subscriberProbe) = initPerformanceSampler()
      ps ! WorkCompleted(1.millisecond)
      ps ! PoolSize(15)
      subscriberProbe.expectMsgType[Sample].poolSize shouldBe startingPoolSize
      ps ! WorkCompleted(1.millisecond)
      ps ! AddSample
      val sample = subscriberProbe.expectMsgType[Sample]
      sample.workDone shouldBe 1
      sample.poolSize shouldBe 15
    }

    "forward metrics to metric reporter" in {
      val reporter = mock[Reporter]
      val p = system.actorOf(MetricsCollector.props(Some(reporter)))
      val ps = PoolSize(3)
      p ! ps

      eventually {
        verify(reporter).report(ps)
        verifyNoMoreInteractions(reporter)
      }
    }

    "report metrics when becoming fully utilized and received Sample" in {
      val reporter = mock[Reporter]
      val mc = system.actorOf(MetricsCollector.props(Some(reporter)))

      mc ! PoolSize(4)
      mc ! fullyUtilizedStatus

      mc ! AddSample

      eventually {
        verify(reporter).report(PoolSize(4))
        verify(reporter).report(PoolUtilized(4))
        verify(reporter).report(WorkQueueLength(fullyUtilizedStatus.queueLength.value))
        verifyNoMoreInteractions(reporter)
      }

    }

  }

}
