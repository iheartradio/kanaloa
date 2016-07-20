package kanaloa.reactive.dispatcher

import akka.actor.{ActorRef, ActorSystem}
import kanaloa.reactive.dispatcher.PerformanceSampler._
import kanaloa.reactive.dispatcher.metrics.Metric._
import kanaloa.reactive.dispatcher.metrics.{MetricsCollector, Reporter}
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually
import org.scalatest.mock.MockitoSugar

import scala.concurrent.duration._

class PerformanceSamplerSpec extends SpecWithActorSystem with MockitoSugar with Eventually {
  val waitDuration = 30.milliseconds

  def initPerformanceSampler(minSampleDurationRatio: Double = 0)(implicit system: ActorSystem): ActorRef = {
    val ps = system.actorOf(MetricsCollector.props(None, PerformanceSamplerSettings(sampleRate = waitDuration / 2, minSampleDurationRatio = minSampleDurationRatio)))
    ps ! fullyUtilizedResult //set it in the busy mode
    ps ! PoolSize(10)
    ps ! Subscribe(self)
    ps
  }
  val partialUtilizedResult: DispatchResult = DispatchResult(1, 0)
  val fullyUtilizedResult: DispatchResult = DispatchResult(0, 2)

  "PerformanceSampler" should {
    "send Samples periodically" in {
      val ps = initPerformanceSampler()
      ps ! WorkCompleted(1.millisecond)
      ps ! WorkCompleted(1.millisecond)

      val sample1 = expectMsgType[Sample]
      sample1.workDone should be(2)

      ps ! WorkCompleted(1.millisecond)

      val sample2 = expectMsgType[Sample]
      sample2.workDone should be(1)

      sample2.start.isAfter(sample1.start) should be(true)

    }

    "ignore metrics when pool isn't fully occupied" in {
      val ps = initPerformanceSampler()
      ps ! partialUtilizedResult
      expectMsgType[PartialUtilization].numOfBusyWorkers should be(9)

      ps ! WorkCompleted(1.millisecond)
      ps ! WorkCompleted(1.millisecond)

      expectNoMsg(waitDuration)

    }

    "ignore Work timeout but include failed Work " in {
      val ps = initPerformanceSampler()
      ps ! WorkTimedOut
      expectNoMsg(waitDuration)
      ps ! WorkFailed
      expectMsgType[Sample].workDone should be(1)
    }

    "resume to collect metrics once pool becomes busy again, but doesn't count old work" in {
      val ps = initPerformanceSampler()
      ps ! partialUtilizedResult

      expectMsgType[PartialUtilization]

      ps ! WorkCompleted(1.millisecond)
      ps ! WorkCompleted(1.millisecond)

      ps ! fullyUtilizedResult

      ps ! WorkCompleted(1.millisecond)

      expectMsgType[Sample].workDone should be(1)

    }

    "reset counter when pool size changed" in {
      val ps = initPerformanceSampler()

      ps ! WorkCompleted(1.millisecond)
      ps ! WorkCompleted(1.millisecond)
      expectMsgType[Sample].workDone should be(2)

      ps ! PoolSize(12)
      ps ! WorkCompleted(1.millisecond)

      val sample = expectMsgType[Sample]
      sample.workDone should be(1)
      sample.poolSize should be(12)

    }

    "register pool size when resting" in {
      val ps = initPerformanceSampler()

      ps ! partialUtilizedResult
      expectMsgType[PartialUtilization]

      ps ! PoolSize(15)
      ps ! fullyUtilizedResult
      ps ! WorkCompleted(1.millisecond)
      expectMsgType[Sample].poolSize should be(15)

    }

    "continue counting when sample duration not long enough" in {
      val ps = initPerformanceSampler(0.99)
      ps ! WorkCompleted(1.millisecond)
      ps ! AddSample
      expectNoMsg(waitDuration / 5)
      ps ! WorkCompleted(1.millisecond)
      expectMsgType[Sample].workDone should be(2)
    }

    "reset counting when pool size changed" in {
      val ps = initPerformanceSampler(0.99)
      ps ! WorkCompleted(1.millisecond)
      ps ! PoolSize(15)
      expectNoMsg(waitDuration / 5)
      ps ! WorkCompleted(1.millisecond)
      val sample = expectMsgType[Sample]
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
        verify(reporter).report(WorkQueueLength(fullyUtilizedResult.workLeft))
        verifyNoMoreInteractions(reporter)
      }

    }

  }

}
