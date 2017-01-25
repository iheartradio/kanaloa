package kanaloa.queue

import java.time.{LocalDateTime ⇒ Time}

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.TestProbe
import kanaloa.SpecWithActorSystem
import kanaloa.Types.QueueLength
import kanaloa.metrics.Metric._
import kanaloa.metrics.Reporter
import kanaloa.queue.QueueSampler.{PartialUtilized, FullyUtilized}
import kanaloa.queue.Sampler._
import kanaloa.queue.WorkerPoolSampler.WorkerPoolSample
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually
import org.scalatest.mock.MockitoSugar

import scala.concurrent.duration._

class WorkerPoolSamplerSpec extends SpecWithActorSystem with MockitoSugar with Eventually {
  val waitDuration = 30.milliseconds
  val startingPoolSize: Int = 10

  def initPerformanceSampler(
    minSampleDurationRatio: Double         = 0,
    sampleInterval:         FiniteDuration = 30.seconds //relies on manual AddSample signal in tests
  )(implicit system: ActorSystem): (ActorRef, TestProbe) = {
    val queueSamplerProbe = TestProbe()
    val ps = system.actorOf(WorkerPoolSampler.props(None, queueSamplerProbe.ref, SamplerSettings(
      sampleInterval = sampleInterval,
      minSampleDurationRatio = minSampleDurationRatio
    ), None))
    ps ! FullyUtilized //set it in the busy mode
    ps ! PoolSize(startingPoolSize)
    val subscriberProbe = TestProbe()
    ps ! Subscribe(subscriberProbe.ref)
    (ps, subscriberProbe)
  }

  "WorkerPoolSampler" should {
    "send Samples periodically" in {
      val (ps, subscriberProbe) = initPerformanceSampler(sampleInterval = 100.milliseconds)
      ps ! WorkCompleted(1.millisecond)
      ps ! WorkCompleted(1.millisecond)

      val sample1 = subscriberProbe.expectMsgType[WorkerPoolSample]
      sample1.workDone shouldBe 2

      ps ! WorkCompleted(1.millisecond)

      val sample2 = subscriberProbe.expectMsgType[WorkerPoolSample]
      sample2.workDone shouldBe 1

      sample2.start.isAfter(sample1.start) shouldBe true

    }

    "collects avgProcess time" in {
      val (ps, subscriberProbe) = initPerformanceSampler(sampleInterval = 100.milliseconds)
      ps ! WorkCompleted(1.millisecond)
      ps ! WorkCompleted(5.millisecond)

      val sample1 = subscriberProbe.expectMsgType[WorkerPoolSample]
      sample1.avgProcessTime should contain(3.milliseconds)

      ps ! WorkCompleted(5.millisecond)
      ps ! WorkCompleted(9.millisecond)
      ps ! WorkCompleted(4.millisecond)

      val sample2 = subscriberProbe.expectMsgType[WorkerPoolSample]
      sample2.avgProcessTime should contain(6.milliseconds)

    }

    "ignore metrics when pool isn't fully occupied" in {
      val (ps, subscriberProbe) = initPerformanceSampler()
      ps ! PartialUtilized

      subscriberProbe.expectMsgType[WorkerPoolSample] //last sample when fully utilized

      ps ! WorkCompleted(1.millisecond)
      ps ! WorkCompleted(1.millisecond)
      ps ! AddSample
      subscriberProbe.expectNoMsg(waitDuration)

    }

    "sends sample without work" in {
      val (ps, subscriberProbe) = initPerformanceSampler()

      ps ! AddSample

      subscriberProbe.expectMsgType[WorkerPoolSample].workDone shouldBe 0

    }

    "continually sends sample without work without reseting start" in {
      val (ps, subscriberProbe) = initPerformanceSampler()

      ps ! AddSample

      val sample1 = subscriberProbe.expectMsgType[WorkerPoolSample]

      Thread.sleep(30) //add a distance between first and second sample
      ps ! Queue.Status(0, QueueLength(4), true)
      ps ! AddSample
      val sample2 = subscriberProbe.expectMsgType[WorkerPoolSample]
      sample2.end.isAfter(sample1.end) shouldBe true
      sample1.start shouldBe sample2.start

    }

    "ignore Work timeout but include failed Work " in {
      val (ps, subscriberProbe) = initPerformanceSampler()
      ps ! WorkTimedOut
      ps ! AddSample
      subscriberProbe.expectMsgType[WorkerPoolSample].workDone shouldBe 0

      ps ! WorkFailed
      ps ! AddSample

      subscriberProbe.expectMsgType[WorkerPoolSample].workDone shouldBe 1
    }

    "resume to collect metrics once pool becomes busy again, but doesn't count old work" in {
      val (ps, subscriberProbe) = initPerformanceSampler()
      ps ! PartialUtilized
      subscriberProbe.expectMsgType[WorkerPoolSample] //last sample when fully utilized

      ps ! WorkCompleted(1.millisecond)
      ps ! WorkCompleted(1.millisecond)

      ps ! FullyUtilized

      ps ! WorkCompleted(1.millisecond)

      ps ! AddSample
      subscriberProbe.expectMsgType[WorkerPoolSample].workDone shouldBe 1

    }

    "reset counter when pool size changed" in {
      val (ps, subscriberProbe) = initPerformanceSampler()

      ps ! WorkCompleted(1.millisecond)
      ps ! WorkCompleted(1.millisecond)

      ps ! AddSample

      subscriberProbe.expectMsgType[WorkerPoolSample].workDone shouldBe 2

      ps ! PoolSize(12)
      subscriberProbe.expectMsgType[WorkerPoolSample].workDone shouldBe 0

      ps ! WorkCompleted(1.millisecond)

      ps ! AddSample

      val sample = subscriberProbe.expectMsgType[WorkerPoolSample]
      sample.workDone shouldBe 1
      sample.poolSize shouldBe 12

    }

    "register pool size when resting" in {
      val (ps, subscriberProbe) = initPerformanceSampler()

      ps ! PartialUtilized
      subscriberProbe.expectMsgType[WorkerPoolSample]

      ps ! PoolSize(15)
      ps ! FullyUtilized
      ps ! WorkCompleted(1.millisecond)
      ps ! AddSample
      subscriberProbe.expectMsgType[WorkerPoolSample].poolSize shouldBe 15

    }

    "continue counting when sample duration not long enough" in {
      val (ps, subscriberProbe) = initPerformanceSampler(0.99, waitDuration)
      ps ! WorkCompleted(1.millisecond)
      ps ! AddSample
      subscriberProbe.expectNoMsg(waitDuration / 3)

      ps ! WorkCompleted(1.millisecond)

      subscriberProbe.expectMsgType[WorkerPoolSample].workDone shouldBe 2
    }

    "reset counting when pool size changed" in {
      val (ps, subscriberProbe) = initPerformanceSampler()
      ps ! WorkCompleted(1.millisecond)
      ps ! PoolSize(15)
      subscriberProbe.expectMsgType[WorkerPoolSample].poolSize shouldBe startingPoolSize
      ps ! WorkCompleted(1.millisecond)
      ps ! AddSample
      val sample = subscriberProbe.expectMsgType[WorkerPoolSample]
      sample.workDone shouldBe 1
      sample.poolSize shouldBe 15
    }

    "forward metrics to metric reporter" in {
      val reporter = mock[Reporter]
      val p =
        factories.workerPoolSampler(reporter = Some(reporter))
      val ps = PoolSize(3)
      p ! ps

      eventually {
        verify(reporter).report(ps)
        verifyNoMoreInteractions(reporter)
      }
    }

    "report metrics when becoming fully utilized and received Sample" in {
      val reporter = mock[Reporter]
      val mc = factories.workerPoolSampler(reporter = Some(reporter))

      mc ! PoolSize(4)
      mc ! FullyUtilized

      mc ! AddSample

      eventually {
        verify(reporter).report(PoolSize(4))
        verify(reporter).report(PoolUtilized(4))
        verifyNoMoreInteractions(reporter)
      }

    }

  }

}
