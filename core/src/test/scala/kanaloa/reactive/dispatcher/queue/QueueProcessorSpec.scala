package kanaloa.reactive.dispatcher.queue

import akka.actor.{ActorRef, ActorRefFactory, PoisonPill, Props}
import akka.testkit.{TestActorRef, TestProbe}
import kanaloa.reactive.dispatcher.ApiProtocol.{QueryStatus, ShutdownSuccessfully, WorkFailed, WorkTimedOut}
import kanaloa.reactive.dispatcher.metrics.{Metric, MetricsCollector}
import kanaloa.reactive.dispatcher.queue.QueueProcessor.{ScaleTo, Shutdown, ShuttingDown, WorkCompleted}
import kanaloa.reactive.dispatcher.{Backend, ResultChecker, SpecWithActorSystem}
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually
import org.scalatest.mock.MockitoSugar

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

class QueueProcessorSpec extends SpecWithActorSystem with Eventually with MockitoSugar {

  type QueueCreator = (ActorRef, Backend, ProcessingWorkerPoolSettings, MetricsCollector, WorkerFactory) ⇒ ResultChecker ⇒ Props
  type QueueTest = (TestActorRef[QueueProcessor], TestProbe, MetricsCollector, TestBackend, TestWorkerFactory) ⇒ Any

  def withQP(poolSettings: ProcessingWorkerPoolSettings, qCreator: QueueCreator, test: QueueTest) {
    val queueProbe = TestProbe("queue")
    val testBackend = new TestBackend()
    val testWorkerFactory = new TestWorkerFactory()
    val metricsCollector = mock[MetricsCollector]
    val qp = TestActorRef[QueueProcessor](qCreator(queueProbe.ref, testBackend, poolSettings, metricsCollector, testWorkerFactory)(SimpleResultChecker))
    watch(qp)
    try {
      test(qp, queueProbe, metricsCollector, testBackend, testWorkerFactory)
    } finally {
      unwatch(qp)
      qp.stop()
    }
  }

  def withQueueProcessor(poolSettings: ProcessingWorkerPoolSettings = ProcessingWorkerPoolSettings())(test: QueueTest) {
    withQP(poolSettings, QueueProcessor.default, test)
  }

  def withQueueProcessorCB(
    poolSettings:           ProcessingWorkerPoolSettings = ProcessingWorkerPoolSettings(),
    circuitBreakerSettings: CircuitBreakerSettings       = CircuitBreakerSettings()
  )(test: QueueTest) {

    val pa: QueueCreator = QueueProcessor.withCircuitBreaker(_: ActorRef, _: Backend, _: ProcessingWorkerPoolSettings, circuitBreakerSettings, _: MetricsCollector, _: WorkerFactory)
    withQP(poolSettings, pa, test)
  }

  "The QueueProcessor" should {

    "create Workers on startup" in withQueueProcessor() { (qp, queueProbe, metricsCollector, testBackend, workerFactory) ⇒
      qp.underlyingActor.workerPool should have size 5
      testBackend.timesInvoked shouldBe 5
    }

    "scale workers up" in withQueueProcessor() { (qp, queueProbe, metricsCollector, testBackend, workerFactory) ⇒
      qp ! ScaleTo(10)
      eventually {
        qp.underlyingActor.workerPool should have size 10
        testBackend.timesInvoked shouldBe 10
      }
    }

    "scale workers down" in withQueueProcessor() { (qp, queueProbe, metricsCollector, testBackend, workerFactory) ⇒

      qp ! ScaleTo(3) //kill 2 Workers

      //can't think of a better way to do this right now.
      val retiredProbes = workerFactory.probeMap.values.filter { probe ⇒
        Try { probe.expectMsg(Worker.Retire) }.toOption.isDefined
      }

      retiredProbes should have size 2

      //kill the 'Workers' who got the Retire message, so that they signal the QP to remove them
      retiredProbes.foreach(_.ref ! PoisonPill)

      eventually {
        qp.underlyingActor.workerPool should have size 3
      }
    }

    "honor minimum pool size during AutoScale" in withQueueProcessor() { (qp, queueProbe, metricsCollector, testBackend, workerFactory) ⇒
      qp ! ScaleTo(1) //minimum is 3

      //can't think of a better way to do this right now.
      val retiredProbes = workerFactory.probeMap.values.filter { probe ⇒
        Try { probe.expectMsg(Worker.Retire) }.toOption.isDefined
      }

      //kill the 'Workers' who got the Retire message, so that they signal the QP to remove them
      retiredProbes.foreach(_.ref ! PoisonPill)

      eventually {
        qp.underlyingActor.workerPool should have size 3
      }
    }

    "honor maximum pool size during AutoScale" in
      withQueueProcessor(ProcessingWorkerPoolSettings(maxPoolSize = 7)) { (qp, queueProbe, metricsCollector, testBackend, workerFactory) ⇒
        qp ! ScaleTo(10) //maximum is 7

        eventually {
          qp.underlyingActor.workerPool should have size 7
          testBackend.timesInvoked shouldBe 7
        }
      }

    "shutdown Queue and Workers" in withQueueProcessor() { (qp, queueProbe, metricsCollector, testBackend, workerFactory) ⇒

      qp ! Shutdown(Some(self), 30.seconds)
      queueProbe.expectMsg(Queue.Retire(30.seconds))

      qp ! QueryStatus()
      expectMsg(ShuttingDown)

      //when the Queue is told to shutDown, it will send
      //term signals to Workers.  Workers will then eventually terminate
      //These PoisonPills simulate that

      workerFactory.probeMap.values.foreach { probe ⇒
        probe.ref ! PoisonPill
      }

      expectMsg(ShutdownSuccessfully)
      expectTerminated(qp)
    }

    "shutdown if Queue terminates" in withQueueProcessor() { (qp, queueProbe, metricsCollector, testBackend, workerFactory) ⇒

      queueProbe.ref ! PoisonPill

      workerFactory.probeMap.values.foreach { probe ⇒
        probe.expectMsg(Worker.Retire)
      }

      qp ! QueryStatus()
      expectMsg(ShuttingDown)

      //simulate the Workers all finishing up
      workerFactory.probeMap.values.foreach { probe ⇒
        probe.ref ! PoisonPill
      }

      expectTerminated(qp)

    }

    //TODO: not sure why this is broken.
    "force shutdown if timeout" ignore withQueueProcessor() { (qp, queueProbe, metricsCollector, testBackend, workerFactory) ⇒

      //watch all Workers
      workerFactory.probeMap.values.foreach { probe ⇒
        watch(probe.ref)
      }

      qp ! Shutdown(Some(self), 25.milliseconds)
      queueProbe.expectMsg(Queue.Retire(25.milliseconds))

      /*expectTerminated(qp) //should force itself to shutdown*/

      //when it forces itself to shutdown, all the Workers should be terminated
      /*workerFactory.probeMap.values.foreach { probe ⇒
        expectTerminated(probe.ref, 10.milliseconds)
      }*/
    }
  }

  "The QueueProcessorWithCircuitBreaker" should {

    "record result history" in withQueueProcessorCB() { (qp, queueProbe, metricsCollector, testBackend, workerFactory) ⇒

      val duration = 1.millisecond

      qp ! WorkCompleted(self, duration)
      qp ! WorkCompleted(self, duration)
      qp ! WorkFailed("")
      qp ! WorkFailed("")
      qp ! WorkTimedOut("")
      qp ! WorkTimedOut("")

      eventually {
        qp.underlyingActor.resultHistory should have size 6
        qp.underlyingActor.resultHistory.count(x ⇒ x) shouldBe 2
        qp.underlyingActor.resultHistory.count(x ⇒ !x) shouldBe 4
      }

      verify(metricsCollector, times(2)).send(Metric.ProcessTime(duration))
      verify(metricsCollector, times(2)).send(Metric.WorkCompleted)
      verify(metricsCollector, times(2)).send(Metric.WorkFailed)
      verify(metricsCollector, times(2)).send(Metric.WorkTimedOut)

      //no Holds should be set since only 4/6 requests failed, which is not the 100% fail rate
      workerFactory.probeMap.values.foreach { probe ⇒
        probe.msgAvailable shouldBe false //is this a race condition waiting to happen?
      }

    }

    "send Holds when the circuitBreaker opens" in withQueueProcessorCB(circuitBreakerSettings = CircuitBreakerSettings(historyLength = 1)) {
      (qp, queueProbe, metricsCollector, testBackend, workerFactory) ⇒
        //we have a queue length of one, and one failure, which sets our error rate to 100%.  Should get Holds for 3 seconds
        qp ! WorkFailed("")
        workerFactory.probeMap.values.foreach { probe ⇒
          probe.expectMsg(Worker.Hold(3.seconds))
        }
    }
  }

  class TestBackend extends Backend {
    val probe = TestProbe()
    var timesInvoked: Int = 0

    override def apply(f: ActorRefFactory): Future[ActorRef] = {
      timesInvoked += 1
      Future.successful(probe.ref)
    }
  }

  class TestWorkerFactory extends WorkerFactory {
    import scala.collection.mutable.{Map ⇒ MMap}

    val probeMap: MMap[ActorRef, TestProbe] = MMap()

    override def createWorker(queueRef: QueueRef, routee: QueueRef, resultChecker: ResultChecker, workerName: String)(implicit ac: ActorRefFactory): ActorRef = {
      val probe = TestProbe(workerName)
      probeMap += (probe.ref → probe)
      probe.ref
    }
  }

}
