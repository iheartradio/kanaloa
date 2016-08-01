package kanaloa.reactive.dispatcher.queue

import akka.actor._
import akka.testkit.{TestActorRef, TestProbe}
import kanaloa.reactive.dispatcher.ApiProtocol.{QueryStatus, ShutdownSuccessfully}
import kanaloa.reactive.dispatcher.metrics.{Metric, MetricsCollector, Reporter}
import kanaloa.reactive.dispatcher.queue.Queue._
import kanaloa.reactive.dispatcher.queue.QueueProcessor.{Shutdown, _}
import kanaloa.reactive.dispatcher.queue.TestUtils._
import kanaloa.reactive.dispatcher.{Backends, SpecWithActorSystem}
import org.scalatest.concurrent.Eventually
import org.scalatest.mock.MockitoSugar

import scala.concurrent.duration._
import scala.util.Random

class QueueSpec extends SpecWithActorSystem {

  "Iterator Queue" should {
    "Process through a list of tasks in sequence with one worker" in new QueueScope {
      val queueProcessor = initQueue(iteratorQueue(List("a", "b", "c").iterator))

      delegatee.expectMsg(DelegateeMessage("a"))
      delegatee.reply(MessageProcessed("a"))
      delegatee.expectMsg(DelegateeMessage("b"))
      delegatee.reply(MessageProcessed("b"))
      delegatee.expectMsg(DelegateeMessage("c"))
      delegatee.reply(MessageProcessed("c"))
    }

    "shutdown with all outstanding work done from the queue side" in new QueueScope {
      val queueProcessor = initQueue(iteratorQueue(List("a", "b", "c", "d").iterator, sendResultsTo = Some(self)))

      delegatee.expectMsg(DelegateeMessage("a"))
      delegatee.reply(MessageProcessed("a"))
      delegatee.expectMsg(DelegateeMessage("b"))

      queueProcessor ! Shutdown(Some(self))

      expectMsg("a")
      expectNoMsg(100.milliseconds) //shouldn't shutdown until the last work is done

      delegatee.reply(MessageProcessed("b"))

      expectMsg("b")

      expectMsg(ShutdownSuccessfully)

    }

    "abandon work when delegatee times out" in new QueueScope {
      val queueProcessor = initQueue(iteratorQueue(List("a", "b").iterator, WorkSettings(timeout = 288.milliseconds)))

      delegatee.expectMsg(DelegateeMessage("a"))

      delegatee.expectNoMsg(250.milliseconds)

      delegatee.expectMsg(DelegateeMessage("b"))
      watch(queueProcessor)
      queueProcessor ! Shutdown

      expectTerminated(queueProcessor)
    }

    "does not retrieve work without workers" in new QueueScope with Backends with MockitoSugar with Eventually {
      import org.mockito.Mockito._
      val iterator = mock[Iterator[String]]
      val queue = system.actorOf(Queue.ofIterator(iterator, metricsCollector, WorkSettings(), Some(self)))

      expectNoMsg(40.milliseconds)
      verifyNoMoreInteractions(iterator)

    }

  }

  "Queue" should {

    "dispatch work on demand on parallel" in new QueueScope {
      val queue = defaultQueue()
      initQueue(queue, numberOfWorkers = 3)

      delegatee.expectNoMsg(40.milliseconds)

      queue ! Enqueue("a")
      delegatee.expectMsg("a")

      queue ! Enqueue("b")
      delegatee.expectMsg("b")

    }

    "won't over burden" in new QueueScope {
      val queue = defaultQueue()
      initQueue(queue, numberOfWorkers = 2)

      queue ! Enqueue("a")
      delegatee.expectMsg("a")

      queue ! Enqueue("b")
      delegatee.expectMsg("b")

      queue ! Enqueue("c")

      delegatee.expectNoMsg(100.milliseconds)
    }

    "reuse workers" in new QueueScope {
      val queue = defaultQueue()
      initQueue(queue, numberOfWorkers = 2)

      queue ! Enqueue("a")
      delegatee.expectMsg("a")
      delegatee.reply(MessageProcessed("a"))

      queue ! Enqueue("b")
      delegatee.expectMsg("b")
      delegatee.reply(MessageProcessed("b"))

      queue ! Enqueue("c")
      delegatee.expectMsg("c")
    }

    "shutdown with all outstanding work done" in new QueueScope {

      val queue = defaultQueue()
      val queueProcessor = initQueue(queue, numberOfWorkers = 2)

      queue ! Enqueue("a")

      delegatee.expectMsg("a")

      queueProcessor ! Shutdown(Some(self))

      expectNoMsg(100.milliseconds) //shouldn't shutdown until the last work is done

      delegatee.reply(MessageProcessed("a"))

      expectMsg(ShutdownSuccessfully)
    }
  }

  "send ack messages when turned on" in new QueueScope {
    val queue = defaultQueue()
    val queueProcessor = initQueue(queue, numberOfWorkers = 2)

    queue ! Enqueue("a", sendAcks = true)
    expectMsg(WorkEnqueued)
    delegatee.expectMsg("a")
  }

  "send results to an actor" in new QueueScope {
    val queue = defaultQueue()
    val queueProcessor = initQueue(queue, numberOfWorkers = 2)

    val sendProbe = TestProbe()

    queue ! Enqueue("a", sendResultsTo = Some(sendProbe.ref))

    delegatee.expectMsg("a")
    delegatee.reply(MessageProcessed("response"))
    sendProbe.expectMsg("response")
  }

  "reject work when retiring" in new QueueScope {
    val queue = defaultQueue()
    watch(queue)
    val queueProcessor = initQueue(queue, numberOfWorkers = 1)
    queue ! Enqueue("a")
    delegatee.expectMsg("a")
    queue ! Enqueue("b")
    delegatee.expectNoMsg()
    //"b" shoud get buffered, since there is only one worker, who is
    //has not "finished" with "a" (we didn't have the probe send back the finished message to the Worker)
    queue ! Retire(50.milliseconds) //give this some time to kill itself
    queue ! Enqueue("c")
    expectMsg(EnqueueRejected(Enqueue("c"), Queue.EnqueueRejected.Retiring))
    //after the the Retiring state is expired, the Queue goes away
    expectTerminated(queue, 75.milliseconds)
    //TODO: need to have more tests for Queue <=> Worker messaging

  }
}

class QueueMetricsSpec extends SpecWithActorSystem with Eventually {

  "Queue Metrics" should {

    "send metric on Enqueue" in new MetricCollectorScope {

      val queue = defaultQueue()
      initQueue(queue, numberOfWorkers = 1)

      queue ! Enqueue("a")

      queue ! Enqueue("b")

      eventually {
        receivedMetrics should contain allOf (Metric.WorkQueueLength(0), Metric.WorkQueueLength(1))
      }
    }

    "send WorkCompleted, ProcessTime, WorkFailed, and WorkTimedOut metrics" in new MetricCollectorScope() {

      val workerProps: Props = Worker.default(
        TestProbe().ref,
        TestProbe().ref,
        TestProbe().ref
      )(resultChecker)

      val queue: QueueRef = defaultQueue(WorkSettings(timeout = 60.milliseconds))
      val processor: ActorRef = TestActorRef(defaultProcessorProps(queue, metricsCollector = metricsCollector))

      watch(processor)

      queue ! Enqueue("a")

      delegatee.expectMsg("a")
      delegatee.reply(MessageProcessed("a"))

      queue ! Enqueue("b")
      delegatee.expectMsg("b")
      delegatee.reply(MessageFailed)

      queue ! Enqueue("c")
      delegatee.expectMsg("c") //timeout this one

      queue ! Enqueue("d")
      delegatee.expectMsg("d")

      eventually {
        receivedMetrics should contain allOf (Metric.WorkFailed, Metric.WorkTimedOut)
        receivedMetrics.collect { case x: Metric.WorkCompleted ⇒ x } should have size 1
      }
    }
  }
}

class QueueScope(implicit system: ActorSystem) extends ScopeWithQueue {
  val metricsCollector: ActorRef = MetricsCollector(None) // To be overridden

  def initQueue(queue: ActorRef, numberOfWorkers: Int = 1, minPoolSize: Int = 1): QueueProcessorRef = {
    val processorProps: Props = defaultProcessorProps(queue, ProcessingWorkerPoolSettings(startingPoolSize = numberOfWorkers, minPoolSize = minPoolSize), metricsCollector)
    system.actorOf(processorProps)
  }

  def iteratorQueue(
    iterator:      Iterator[String],
    workSetting:   WorkSettings     = WorkSettings(),
    sendResultsTo: Option[ActorRef] = None
  ): QueueRef =
    system.actorOf(
      iteratorQueueProps(iterator, metricsCollector, workSetting, sendResultsTo),
      "iterator-queue-" + Random.nextInt(100000)
    )

  def defaultQueue(workSetting: WorkSettings = WorkSettings()): QueueRef =
    system.actorOf(
      Queue.default(metricsCollector, workSetting),
      "default-queue-" + Random.nextInt(100000)
    )

}

class MetricCollectorScope(implicit system: ActorSystem) extends QueueScope {
  @volatile
  var receivedMetrics: List[Metric] = Nil

  override val metricsCollector: ActorRef = MetricsCollector(Some(new Reporter {
    def report(metric: Metric): Unit = receivedMetrics = metric :: receivedMetrics
  }))

}

