package kanaloa.reactive.dispatcher.metrics

import akka.actor.{ActorSystem, ActorRef}
import kanaloa.reactive.dispatcher.SpecWithActorSystem
import kanaloa.reactive.dispatcher.metrics.Metric._
import kanaloa.reactive.dispatcher.metrics.MetricsCollector._
import org.scalatest.time.{Second, Millis, Span}
import concurrent.duration._
import org.scalatest.concurrent.Eventually._

class MetricsCollectorSpec extends SpecWithActorSystem {
  val waitDuration = 30.milliseconds
  def initMetricsCollector(minSampleDurationRatio: Double = 0)(implicit system: ActorSystem): ActorRef = {
    val mc = system.actorOf(MetricsCollector.props(None, Settings(sampleRate = waitDuration / 2, minSampleDurationRatio = minSampleDurationRatio)))
    mc ! PoolIdle(0)
    mc ! PoolSize(10)
    mc ! Subscribe(self)
    mc
  }

  "MetricsCollector" should {
    "sample very sample rate" in {
      val mc = initMetricsCollector()
      mc ! WorkCompleted
      mc ! WorkCompleted

      val sample1 = expectMsgType[Sample]
      sample1.workDone should be(2)

      mc ! WorkCompleted

      val sample2 = expectMsgType[Sample]
      sample2.workDone should be(1)

      sample2.start.isAfter(sample1.start) should be(true)

    }

    "ignore metrics when pool isn't fully occupied" in {
      val mc = initMetricsCollector()
      mc ! PoolIdle(1)
      expectMsgType[PartialUtilization].numOfBusyWorkers should be(9)

      mc ! WorkCompleted
      mc ! WorkCompleted

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
      mc ! PoolIdle(1)

      expectMsgType[PartialUtilization]

      mc ! WorkCompleted
      mc ! WorkCompleted

      mc ! PoolIdle(0)

      mc ! WorkCompleted

      expectMsgType[Sample].workDone should be(1)

    }

    "reset counter when pool size changed" in {
      val mc = initMetricsCollector()

      mc ! WorkCompleted
      mc ! WorkCompleted
      expectMsgType[Sample].workDone should be(2)

      mc ! PoolSize(12)
      mc ! WorkCompleted

      val sample = expectMsgType[Sample]
      sample.workDone should be(1)
      sample.poolSize should be(12)

    }

    "register pool size when resting" in {
      val mc = initMetricsCollector()

      mc ! PoolIdle(1)
      expectMsgType[PartialUtilization]

      mc ! PoolSize(15)
      mc ! PoolIdle(0)
      mc ! WorkCompleted
      expectMsgType[Sample].poolSize should be(15)

    }

    "continue counting when sample duration not long enough" in {
      val mc = initMetricsCollector(0.99)
      mc ! WorkCompleted
      mc ! AddSample
      expectNoMsg(waitDuration / 5)
      mc ! WorkCompleted
      expectMsgType[Sample].workDone should be(2)
    }

    "reset counting when pool size changed" in {
      val mc = initMetricsCollector(0.99)
      mc ! WorkCompleted
      mc ! PoolSize(15)
      expectNoMsg(waitDuration / 5)
      mc ! WorkCompleted
      val sample = expectMsgType[Sample]
      sample.workDone should be(1)
      sample.poolSize should be(15)
    }

  }

}
