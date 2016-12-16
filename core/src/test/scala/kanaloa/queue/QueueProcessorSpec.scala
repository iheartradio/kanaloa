package kanaloa.queue

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.Status.Success
import akka.actor.{ActorRef, ActorRefFactory, PoisonPill, Props}
import akka.testkit.TestActor.AutoPilot
import akka.testkit.{TestActorRef, TestActor, TestProbe}
import kanaloa.ApiProtocol.{ShutdownGracefully, ShutdownForcefully, QueryStatus, ShutdownSuccessfully}
import kanaloa.handler.{ResultChecker, Handler, GeneralActorRefHandler, HandlerProvider}
import kanaloa.metrics.Metric
import kanaloa.metrics.Metric.PoolSize
import kanaloa.queue.Queue.Retire
import kanaloa.queue.QueueProcessor.{ScaleTo, Shutdown, ShuttingDown}
import kanaloa._
import kanaloa.queue.TestUtils.{MessageProcessed, DelegateeMessage}
import org.scalatest.concurrent.Eventually
import scala.collection.mutable.{Map ⇒ MMap}
import scala.concurrent.{Promise, Future}
import scala.concurrent.duration._
import HandlerProviders._
class QueueProcessorSpec extends SpecWithActorSystem with Eventually with Backends {

  type QueueTest = (TestActorRef[QueueProcessor[Any]], TestProbe, TestProbe, HandlerProvider[Any], TestWorkerFactory) ⇒ Any

  def withQueueProcessor(poolSettings: ProcessingWorkerPoolSettings = ProcessingWorkerPoolSettings(defaultShutdownTimeout = 500.milliseconds))(test: QueueTest) {

    val queueProbe = TestProbe("queue")
    val serviceProbe = TestProbe("service")
    val testHandlerProvider = HandlerProviders.simpleHandlerProvider(serviceProbe)
    val testWorkerFactory = new TestWorkerFactory()
    val metricsCollector = TestProbe("metrics-collector")
    val qp = TestActorRef[QueueProcessor[Any]](QueueProcessor.default(queueProbe.ref, testHandlerProvider, poolSettings, metricsCollector.ref, None, testWorkerFactory))

    eventually {
      qp.underlyingActor.workerPool should have size poolSettings.startingPoolSize.toLong
    }

    watch(qp)
    try {
      test(qp, queueProbe, metricsCollector, testHandlerProvider, testWorkerFactory)
    } finally {
      unwatch(qp)
      qp.stop()
    }
  }

  //very specific for my needs here, but we can def generalize this if need be
  implicit class HelpedTestProbe(probe: TestProbe) {

    def setAutoPilotPF(pf: PartialFunction[Any, AutoPilot]): Unit = {
      probe.setAutoPilot(
        new AutoPilot {
          override def run(sender: QueueRef, msg: Any): AutoPilot = pf.applyOrElse(msg, (x: Any) ⇒ TestActor.NoAutoPilot)
        }
      )
    }
  }

  "The QueueProcessor" should {

    "create Workers on startup" in withQueueProcessor(ProcessingWorkerPoolSettings(startingPoolSize = 5)) { (qp, queueProbe, metricsCollector, testBackend, workerFactory) ⇒
      eventually {
        qp.underlyingActor.workerPool should have size 5
      }
    }

    "report pool size on startup" in withQueueProcessor() { (qp, queueProbe, metricsCollector, testBackend, workerFactory) ⇒
      metricsCollector.expectMsg(PoolSize(0))
    }

    "scale workers up" in withQueueProcessor() { (qp, queueProbe, metricsCollector, testBackend, workerFactory) ⇒
      qp ! ScaleTo(10)
      eventually {
        qp.underlyingActor.workerPool should have size 10
      }
    }

    "does not scale workers when there is already workers creation in flight" in new Backends {
      import system.dispatcher
      val promise = Promise[ActorRef]
      val handlerProvider = fromPromise(promise, simpleResultChecker)
      val metricsCollectorProbe = TestProbe()
      val qp = TestActorRef[QueueProcessor[Any]](QueueProcessor.default(
        TestProbe().ref,
        handlerProvider,
        ProcessingWorkerPoolSettings(startingPoolSize = 1),
        metricsCollectorProbe.ref
      ))

      qp ! ScaleTo(10) //this should be ignored

      promise.success(TestProbe().ref)

      metricsCollectorProbe.expectMsg(PoolSize(0))
      metricsCollectorProbe.expectMsg(PoolSize(1))
      metricsCollectorProbe.expectNoMsg(30.milliseconds)

    }

    "scale workers down" in withQueueProcessor() { (qp, queueProbe, metricsCollector, testBackend, workerFactory) ⇒

      qp ! ScaleTo(4) //kill 1 Worker

      eventually {
        workerFactory.retiredCount.get() shouldBe 1
      }

      //pick any 2 actors, since the QueueProcessor is not currently tracking who got the term signal
      //kill the 'Workers' who got the Retire message, so that they signal the QP to remove them
      workerFactory.probeMap.values.take(1).foreach(_.ref ! PoisonPill)

      eventually {
        qp.underlyingActor.workerPool should have size 4
      }
      //just to be safe(to make sure that some other Retire messages didn't sneak by after we reached 2 earlier)
      workerFactory.retiredCount.get shouldBe 1
    }

    "honor minimum pool size during AutoScale" in withQueueProcessor() { (qp, queueProbe, metricsCollector, testBackend, workerFactory) ⇒
      qp ! ScaleTo(1) //minimum is 3, starts at 5

      eventually {
        workerFactory.retiredCount.get() shouldBe 2
      }

      workerFactory.probeMap.values.take(2).foreach(_.ref ! PoisonPill)

      eventually {
        qp.underlyingActor.workerPool should have size 3
      }
    }

    "honor maximum pool size during AutoScale" in
      withQueueProcessor(ProcessingWorkerPoolSettings(maxPoolSize = 7)) { (qp, queueProbe, metricsCollector, testBackend, workerFactory) ⇒
        qp ! ScaleTo(10) //maximum is 7

        eventually {
          qp.underlyingActor.workerPool should have size 7
        }
      }

    "attempt to keep the number of Workers at the minimumWorkers when worker dies" in withQueueProcessor(ProcessingWorkerPoolSettings(healthCheckInterval = 10.milliseconds)) {
      (qp, queueProbe, metricsCollector, testBackend, workerFactory) ⇒
        //current workers are 5, minimum workers are 3, so killing 4 should result in 2 new recreate attempts
        workerFactory.probeMap.keys.take(4).foreach(workerFactory.killAndRemoveWorker)
        eventually {
          qp.underlyingActor.workerPool should have size 3
          workerFactory.probeMap should have size 3 //should only be 3 workers
        }
    }

    "attempt to retry create Workers until it hits the minimumWorkers" in {
      val settings = ProcessingWorkerPoolSettings(minPoolSize = 2, startingPoolSize = 2, healthCheckInterval = 10.milliseconds)
      import system.dispatcher
      val promise = Promise[ActorRef]
      val handlerProvider = fromPromise(promise, simpleResultChecker)

      val qp = TestActorRef[QueueProcessor[Any]](QueueProcessor.default(
        TestProbe("queue").ref,
        handlerProvider, settings, TestProbe("metrics-collector").ref
      ))
      expectNoMsg(200.milliseconds)
      promise.complete(scala.util.Success(TestProbe().ref))
      eventually {
        qp.underlyingActor.workerPool should have size 2
      }
    }

    "shutdown itself when all worker dies" in withQueueProcessor() { (qp, _, _, _, workerFactory) ⇒
      watch(qp)
      workerFactory.killsAllWorkers()
      expectTerminated(qp)
    }

    "shutdown Queue and wait for Workers to terminate" in withQueueProcessor() { (qp, queueProbe, metricsCollector, testBackend, workerFactory) ⇒

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

      eventually {
        workerFactory.retiredCount.get() shouldBe 5 //all workers should receive a Retire signal
      }

      qp ! QueryStatus()
      expectMsg(ShuttingDown)

      //simulate the Workers all finishing up
      workerFactory.probeMap.values.foreach { probe ⇒
        probe.ref ! PoisonPill
      }

      expectTerminated(qp)

    }

    "shutdown before worker created" in {
      import system.dispatcher

      val queueProbe = TestProbe()
      val queueProcessor = system.actorOf(
        QueueProcessor.default(
          queueProbe.ref,
          fromPromise(Promise[ActorRef], ResultChecker.complacent),
          ProcessingWorkerPoolSettings(),
          TestProbe().ref
        )
      )
      queueProcessor ! Shutdown(Some(self))
      queueProbe.expectMsgType[Retire]
      queueProbe.ref ! PoisonPill

      expectMsg(ShutdownSuccessfully)

    }

    "force shutdown if timeout" in withQueueProcessor() { (qp, queueProbe, metricsCollector, testBackend, workerFactory) ⇒

      qp ! Shutdown(Some(self), 25.milliseconds)
      queueProbe.expectMsg(Queue.Retire(25.milliseconds))
      //We wn't kill the Workers, and the timeout should kick in
      expectMsg(ShutdownForcefully)
      expectTerminated(qp) //should force itself to shutdown
    }
  }

  class TestWorkerFactory extends WorkerFactory {

    val probeMap: MMap[ActorRef, TestProbe] = MMap()

    val retiredCount: AtomicInteger = new AtomicInteger(0)

    //create a Worker, and increment a count when its told to Retire.
    override def createWorker[T](
      queueRef:               QueueRef,
      handler:                Handler[T],
      metricsCollector:       ActorRef,
      circuitBreakerSettings: Option[CircuitBreakerSettings],
      workerName:             String
    )(implicit ac: ActorRefFactory): ActorRef = {
      val probe = TestProbe(workerName)
      probe.setAutoPilotPF {
        case Worker.Retire ⇒
          retiredCount.incrementAndGet()
          //probe.ref
          TestActor.NoAutoPilot
      }
      probeMap += (probe.ref → probe)
      probe.ref
    }

    def killAndRemoveWorker(ref: ActorRef): Unit = {
      probeMap.remove(ref)
      ref ! PoisonPill
    }

    def killsAllWorkers(): Unit = {
      probeMap.keys.foreach(killAndRemoveWorker)
    }
  }

}

class AutothrottleWhenWorkingSpec extends SpecWithActorSystem with Eventually {

  "autothrottle" should {

    "send PoolSize metric when pool size changes" in new MetricCollectorScope {

      val queueProcessor = initQueue(
        iteratorQueue(Iterator("a", "b")), //make sure queue remains alive during test
        numberOfWorkers = 1
      )
      queueProcessor ! ScaleTo(3)
      queueProcessor ! ScaleTo(5)

      eventually {
        val poolSizeMetrics = receivedMetrics.collect {
          case Metric.PoolSize(x) ⇒ x
        }
        poolSizeMetrics.max should be <= 5
      }
    }

    "retiring a worker when there is no work" in new QueueScope {
      val queueProcessor = initQueue(
        iteratorQueue(
          List("a", "b", "c").iterator,
          sendResultsTo = Some(self)
        ),
        numberOfWorkers = 2
      )
      queueProcessor ! ScaleTo(1)
      expectNoMsg(20.millisecond) //wait for retire to take effect
      delegatee.expectMsgType[DelegateeMessage]

      delegatee.reply(MessageProcessed("ar"))

      expectMsg("ar")
      delegatee.expectMsgType[DelegateeMessage]

      delegatee.reply(MessageProcessed("br"))
      expectMsg("br")

    }

    "retiring a worker when it already started working" in new QueueScope {
      val queueProcessor = initQueue(
        iteratorQueue(
          List("a", "b", "c").iterator,
          sendResultsTo = Some(self)
        ),
        numberOfWorkers = 2
      )
      delegatee.expectMsgType[DelegateeMessage]

      expectNoMsg(20.millisecond) //wait for both workers get occupied

      queueProcessor ! ScaleTo(1)

      expectNoMsg(20.millisecond) //wait for one of the workers got into retiring

      delegatee.reply(MessageProcessed("ar"))

      delegatee.expectMsgType[DelegateeMessage]

      expectMsg("ar")

      delegatee.reply(MessageProcessed("br"))
      expectMsg("br")

    }
  }
}
