package kanaloa.queue

import java.time.{LocalDateTime ⇒ Time}

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.TestProbe
import kanaloa.SpecWithActorSystem
import kanaloa.Types.QueueLength
import kanaloa.metrics.{Metric, Reporter}
import kanaloa.queue.QueueSampler.{Overflown, PartialUtilized, QueueSample}
import kanaloa.queue.Sampler.{AddSample, Subscribe, SamplerSettings}
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually
import org.scalatest.mock.MockitoSugar

import scala.concurrent.duration._

class QueueSamplerSpec extends SpecWithActorSystem with MockitoSugar with Eventually {
  val waitDuration = 30.milliseconds
  val startingPoolSize: Int = 10
  val defaultSampleInterval = 20.milliseconds
  def initQueueSampler(
    minSampleDurationRatio: Double         = 0,
    sampleInterval:         FiniteDuration = defaultSampleInterval,
    autoSampling:           Boolean        = false //relies on manual AddSample signal in tests
  )(implicit system: ActorSystem): (ActorRef, TestProbe) = {
    val ps = system.actorOf(QueueSampler.props(None, SamplerSettings(
      sampleInterval = sampleInterval,
      minSampleDurationRatio = minSampleDurationRatio
    ), autoSampling))
    ps ! overflownStatus //set it in the busy mode
    val subscriberProbe = TestProbe()
    ps ! Subscribe(subscriberProbe.ref)
    (ps, subscriberProbe)
  }
  val partialUtilizedStatus: Queue.Status = Queue.Status(1, QueueLength(0), false)
  val overflownStatus: Queue.Status = Queue.Status(0, QueueLength(2), true)

  def dispatchReport(dispatched: Int = 1, status: Queue.Status = overflownStatus) = Queue.DispatchReport(status, dispatched)

  "QueueSampler" should {
    "send Samples periodically" in {
      val (ps, subscriberProbe) = initQueueSampler(sampleInterval = 100.milliseconds, autoSampling = true)
      ps ! dispatchReport()
      ps ! dispatchReport()

      val sample1 = subscriberProbe.expectMsgType[QueueSample]
      sample1.dequeued shouldBe 2

      ps ! dispatchReport()

      val sample2 = subscriberProbe.expectMsgType[QueueSample]
      sample2.dequeued shouldBe 1

      sample2.start.isAfter(sample1.start) shouldBe true

    }

    "not report PartialUtilized for sporadic workBuffered report" in {
      val (ps, subscriberProbe) = initQueueSampler()

      ps ! dispatchReport(status = partialUtilizedStatus)

      subscriberProbe.expectNoMsg(waitDuration)

    }

    "ignore metrics when pool isn't fully occupied" in {
      val (ps, subscriberProbe) = initQueueSampler()

      ps ! dispatchReport(status = partialUtilizedStatus)
      expectNoMsg(defaultSampleInterval * 2)
      ps ! dispatchReport(status = partialUtilizedStatus)
      subscriberProbe.expectMsg(PartialUtilized)

      ps ! AddSample
      subscriberProbe.expectNoMsg(waitDuration)

    }

    "sends sample without work" in {
      val (ps, subscriberProbe) = initQueueSampler()

      ps ! AddSample

      subscriberProbe.expectMsgType[QueueSample].dequeued shouldBe 0

    }

    "continually sends sample without work without reseting start" in {
      val (ps, subscriberProbe) = initQueueSampler()

      ps ! AddSample

      val sample1 = subscriberProbe.expectMsgType[QueueSample]

      Thread.sleep(30) //add a distance between first and second sample
      ps ! Queue.Status(0, QueueLength(4), true)
      ps ! AddSample
      val sample2 = subscriberProbe.expectMsgType[QueueSample]
      sample2.end.isAfter(sample1.end) shouldBe true
      sample1.start shouldBe sample2.start
      sample2.queueLength shouldBe QueueLength(4)

    }

    "resume to collect metrics once pool becomes busy again, also send overflown report but doesn't count old work" in {
      val (ps, subscriberProbe) = initQueueSampler()

      ps ! dispatchReport(status = partialUtilizedStatus)
      expectNoMsg(defaultSampleInterval * 2)
      ps ! dispatchReport(status = partialUtilizedStatus)

      subscriberProbe.expectMsg(PartialUtilized)

      ps ! overflownStatus

      ps ! dispatchReport()

      ps ! AddSample
      subscriberProbe.expectMsg(Overflown)
      subscriberProbe.expectMsgType[QueueSample].dequeued shouldBe 1

    }

    "register queue length" in {
      val (ps, subscriberProbe) = initQueueSampler()

      ps ! dispatchReport(status = Queue.Status(0, QueueLength(21), true))
      ps ! AddSample

      subscriberProbe.expectMsgType[QueueSample].queueLength shouldBe QueueLength(21)

    }

    "continue counting when sample duration not long enough" in {
      val (ps, subscriberProbe) = initQueueSampler(0.99, waitDuration, true)
      ps ! dispatchReport()
      ps ! AddSample
      subscriberProbe.expectNoMsg(waitDuration / 3)

      ps ! dispatchReport()

      subscriberProbe.expectMsgType[QueueSample].dequeued shouldBe 2
    }

    "forward metrics to metric reporter" in {
      val reporter = mock[Reporter]
      val p = system.actorOf(QueueSampler.props(Some(reporter)))
      val ps = Metric.WorkReceived
      p ! ps

      eventually {
        verify(reporter).report(ps)
        verifyNoMoreInteractions(reporter)
      }
    }

    "report metrics when becoming fully utilized and received Sample" in {
      val reporter = mock[Reporter]
      val mc = system.actorOf(QueueSampler.props(Some(reporter)))

      mc ! overflownStatus
      mc ! Metric.WorkReceived

      mc ! AddSample

      eventually {
        verify(reporter).report(Metric.WorkReceived)
        verify(reporter).report(Metric.WorkQueueLength(overflownStatus.queueLength.value))
        verifyNoMoreInteractions(reporter)
      }

    }

  }

}
