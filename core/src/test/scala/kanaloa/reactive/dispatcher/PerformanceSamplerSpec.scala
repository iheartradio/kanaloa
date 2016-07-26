package kanaloa.reactive.dispatcher

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.TestProbe
import kanaloa.reactive.dispatcher.PerformanceSampler._
import kanaloa.reactive.dispatcher.Types.QueueLength
import kanaloa.reactive.dispatcher.metrics.Metric._
import kanaloa.reactive.dispatcher.metrics.{MetricsCollector, Reporter}
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually
import org.scalatest.mock.MockitoSugar
import java.time.{LocalDateTime ⇒ Time, LocalDate}
import scala.concurrent.duration._

class PerformanceSamplerSpec extends SpecWithActorSystem with MockitoSugar with Eventually {
  val waitDuration = 30.milliseconds

  def initPerformanceSampler(
    minSampleDurationRatio: Double  = 0,
    reportNoProgress:       Boolean = false
  )(implicit system: ActorSystem): (ActorRef, TestProbe) = {
    val ps = system.actorOf(MetricsCollector.props(None, PerformanceSamplerSettings(
      sampleInterval = waitDuration / 2,
      minSampleDurationRatio = minSampleDurationRatio,
      reportNoProgress
    )))
    ps ! fullyUtilizedResult //set it in the busy mode
    ps ! PoolSize(10)
    val subscriberProbe = TestProbe()
    ps ! Subscribe(subscriberProbe.ref)
    (ps, subscriberProbe)
  }
  val partialUtilizedResult: DispatchResult = DispatchResult(1, QueueLength(0), false)
  val fullyUtilizedResult: DispatchResult = DispatchResult(0, QueueLength(2), true)

  "PerformanceSampler" should {
    "send Samples periodically" in {
      val (ps, subscriberProbe) = initPerformanceSampler()
      ps ! WorkCompleted(1.millisecond)
      ps ! WorkCompleted(1.millisecond)

      val sample1 = subscriberProbe.expectMsgType[Sample]
      sample1.workDone should be(2)

      ps ! WorkCompleted(1.millisecond)

      val sample2 = subscriberProbe.expectMsgType[Sample]
      sample2.workDone should be(1)

      sample2.start.isAfter(sample1.start) should be(true)

    }

    "ignore metrics when pool isn't fully occupied" in {
      val (ps, subscriberProbe) = initPerformanceSampler()
      ps ! partialUtilizedResult
      subscriberProbe.expectMsgType[PartialUtilization].numOfBusyWorkers should be(9)

      ps ! WorkCompleted(1.millisecond)
      ps ! WorkCompleted(1.millisecond)

      subscriberProbe.expectNoMsg(waitDuration)

    }

    "ignore Work timeout but include failed Work " in {
      val (ps, subscriberProbe) = initPerformanceSampler()
      ps ! WorkTimedOut
      subscriberProbe.expectNoMsg(waitDuration)
      ps ! WorkFailed
      subscriberProbe.expectMsgType[Sample].workDone should be(1)
    }

    "report NoProgress when no work done" in {
      val (ps, subscriberProbe) = initPerformanceSampler(reportNoProgress = true)
      subscriberProbe.expectMsgType[NoProgress].since.isBefore(Time.now) should be(true)
    }

    "resume to collect metrics once pool becomes busy again, but doesn't count old work" in {
      val (ps, subscriberProbe) = initPerformanceSampler()
      ps ! partialUtilizedResult

      subscriberProbe.expectMsgType[PartialUtilization]

      ps ! WorkCompleted(1.millisecond)
      ps ! WorkCompleted(1.millisecond)

      ps ! fullyUtilizedResult

      ps ! WorkCompleted(1.millisecond)

      subscriberProbe.expectMsgType[Sample].workDone should be(1)

    }

    "reset counter when pool size changed" in {
      val (ps, subscriberProbe) = initPerformanceSampler()

      ps ! WorkCompleted(1.millisecond)
      ps ! WorkCompleted(1.millisecond)

      subscriberProbe.expectMsgType[Sample].workDone should be(2)

      ps ! PoolSize(12)
      ps ! WorkCompleted(1.millisecond)

      val sample = subscriberProbe.expectMsgType[Sample]
      sample.workDone should be(1)
      sample.poolSize should be(12)

    }

    "remember queue length when pool size changed" in {
      val (ps, subscriberProbe) = initPerformanceSampler()

      ps ! DispatchResult(0, QueueLength(11), true)
      ps ! WorkCompleted(1.millisecond)

      subscriberProbe.expectMsgType[Sample].queueLength.value should be(11)

      ps ! PoolSize(12)
      ps ! WorkCompleted(1.millisecond)

      subscriberProbe.expectMsgType[Sample].queueLength.value should be(11)
    }

    "register pool size when resting" in {
      val (ps, subscriberProbe) = initPerformanceSampler()

      ps ! partialUtilizedResult
      subscriberProbe.expectMsgType[PartialUtilization]

      ps ! PoolSize(15)
      ps ! fullyUtilizedResult
      ps ! WorkCompleted(1.millisecond)
      subscriberProbe.expectMsgType[Sample].poolSize should be(15)

    }

    "register queue length" in {
      val (ps, subscriberProbe) = initPerformanceSampler()

      ps ! DispatchResult(0, QueueLength(21), true)
      ps ! WorkCompleted(1.millisecond)

      subscriberProbe.expectMsgType[Sample].queueLength shouldBe QueueLength(21)

    }

    "continue counting when sample duration not long enough" in {
      val (ps, subscriberProbe) = initPerformanceSampler(0.99)
      ps ! WorkCompleted(1.millisecond)
      ps ! AddSample
      subscriberProbe.expectNoMsg(waitDuration / 5)
      ps ! WorkCompleted(1.millisecond)
      subscriberProbe.expectMsgType[Sample].workDone should be(2)
    }

    "reset counting when pool size changed" in {
      val (ps, subscriberProbe) = initPerformanceSampler(0.99)
      ps ! WorkCompleted(1.millisecond)
      ps ! PoolSize(15)
      subscriberProbe.expectNoMsg(waitDuration / 5)
      ps ! WorkCompleted(1.millisecond)
      val sample = subscriberProbe.expectMsgType[Sample]
      sample.workDone should be(1)
      sample.poolSize should be(15)
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
      mc ! fullyUtilizedResult

      mc ! AddSample

      eventually {
        verify(reporter).report(PoolSize(4))
        verify(reporter).report(PoolUtilized(4))
        verify(reporter).report(WorkQueueLength(fullyUtilizedResult.queueLength.value))
        verifyNoMoreInteractions(reporter)
      }

    }

  }

}
