package kanaloa.reactive.dispatcher.metrics

import akka.actor.{ActorSystem, ActorRef}
import kanaloa.reactive.dispatcher.SpecWithActorSystem
import kanaloa.reactive.dispatcher.metrics.Metric._
import kanaloa.reactive.dispatcher.metrics.MetricsCollector._
import org.scalatest.concurrent.Eventually
import org.scalatest.mock.MockitoSugar
import concurrent.duration._
import org.mockito.Mockito._

class MetricsCollectorSpec extends SpecWithActorSystem with MockitoSugar with Eventually {
  val waitDuration = 30.milliseconds

  def initMetricsCollector(minSampleDurationRatio: Double = 0)(implicit system: ActorSystem): ActorRef = {
    val mc = system.actorOf(MetricsCollector.props(None, MetricsCollectorSettings(sampleRate = waitDuration / 2, minSampleDurationRatio = minSampleDurationRatio)))
    mc ! fullyUtilizedResult //set it in the busy mode
    mc ! PoolSize(10)
    mc ! Subscribe(self)
    mc
  }
  val partialUtilizedResult: DispatchResult = DispatchResult(1, 0)
  val fullyUtilizedResult: DispatchResult = DispatchResult(0, 2)

  "MetricsCollector" should {
    "send Samples periodically" in {
      val mc = initMetricsCollector()
      mc ! WorkCompleted(1.millisecond)
      mc ! WorkCompleted(1.millisecond)

      val sample1 = expectMsgType[Sample]
      sample1.workDone should be(2)

      mc ! WorkCompleted(1.millisecond)

      val sample2 = expectMsgType[Sample]
      sample2.workDone should be(1)

      sample2.start.isAfter(sample1.start) should be(true)

    }

    "ignore metrics when pool isn't fully occupied" in {
      val mc = initMetricsCollector()
      mc ! partialUtilizedResult
      expectMsgType[PartialUtilization].numOfBusyWorkers should be(9)

      mc ! WorkCompleted(1.millisecond)
      mc ! WorkCompleted(1.millisecond)

      expectNoMsg(waitDuration)

    }

    "ignore Work timeout but include failed Work " in {
      val mc = initMetricsCollector()
      mc ! WorkTimedOut
      expectNoMsg(waitDuration)
      mc ! WorkFailed
      expectMsgType[Sample].workDone should be(1)
    }

    "resume to collect metrics once pool becomes busy again, but doesn't count old work" in {
      val mc = initMetricsCollector()
      mc ! partialUtilizedResult

      expectMsgType[PartialUtilization]

      mc ! WorkCompleted(1.millisecond)
      mc ! WorkCompleted(1.millisecond)

      mc ! fullyUtilizedResult

      mc ! WorkCompleted(1.millisecond)

      expectMsgType[Sample].workDone should be(1)

    }

    "reset counter when pool size changed" in {
      val mc = initMetricsCollector()

      mc ! WorkCompleted(1.millisecond)
      mc ! WorkCompleted(1.millisecond)
      expectMsgType[Sample].workDone should be(2)

      mc ! PoolSize(12)
      mc ! WorkCompleted(1.millisecond)

      val sample = expectMsgType[Sample]
      sample.workDone should be(1)
      sample.poolSize should be(12)

    }

    "register pool size when resting" in {
      val mc = initMetricsCollector()

      mc ! partialUtilizedResult
      expectMsgType[PartialUtilization]

      mc ! PoolSize(15)
      mc ! fullyUtilizedResult
      mc ! WorkCompleted(1.millisecond)
      expectMsgType[Sample].poolSize should be(15)

    }

    "continue counting when sample duration not long enough" in {
      val mc = initMetricsCollector(0.99)
      mc ! WorkCompleted(1.millisecond)
      mc ! AddSample
      expectNoMsg(waitDuration / 5)
      mc ! WorkCompleted(1.millisecond)
      expectMsgType[Sample].workDone should be(2)
    }

    "reset counting when pool size changed" in {
      val mc = initMetricsCollector(0.99)
      mc ! WorkCompleted(1.millisecond)
      mc ! PoolSize(15)
      expectNoMsg(waitDuration / 5)
      mc ! WorkCompleted(1.millisecond)
      val sample = expectMsgType[Sample]
      sample.workDone should be(1)
      sample.poolSize should be(15)
    }

    "forward metrics to metric reporter" in {
      val reporter = mock[Reporter]
      val mc = system.actorOf(MetricsCollector.props(Some(reporter)))
      val ps = PoolSize(3)
      mc ! ps

      verify(reporter).report(ps)
    }

    "report utilization even when fully utilized" in {
      val reporter = mock[Reporter]
      val mc = system.actorOf(MetricsCollector.props(Some(reporter)))

      mc ! PoolSize(4)
      mc ! fullyUtilizedResult

      mc ! AddSample

      eventually {
        verify(reporter).report(PoolSize(4))
        verify(reporter).report(PoolUtilized(4))
      }

    }

  }

}
