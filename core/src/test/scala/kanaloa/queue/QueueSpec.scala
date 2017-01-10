package kanaloa.queue

import akka.actor._
import akka.testkit.{TestActorRef, TestProbe}
import kanaloa.ApiProtocol.{QueryStatus, ShutdownSuccessfully}
import kanaloa.handler.GeneralActorRefHandler
import kanaloa.metrics.{Metric, Reporter}
import kanaloa.queue.Queue._
import kanaloa.queue.WorkerPoolManager.{Shutdown, _}
import kanaloa.queue.TestUtils._
import kanaloa.{QueueSampler, MockServices, SpecWithActorSystem}
import org.scalatest.concurrent.Eventually
import org.scalatest.mock.MockitoSugar

import scala.concurrent.duration._
import scala.util.Random

class QueueSpec extends SpecWithActorSystem {
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
      val workerPoolManager = initQueue(queue, numberOfWorkers = 2)

      queue ! Enqueue("a")

      delegatee.expectMsg("a")

      workerPoolManager ! Shutdown(Some(self))

      expectNoMsg(100.milliseconds) //shouldn't shutdown until the last work is done

      delegatee.reply(MessageProcessed("a"))

      expectMsg(ShutdownSuccessfully)
    }
  }

  "send ack messages when turned on" in new QueueScope {
    val queue = defaultQueue()
    val workerPoolManager = initQueue(queue, numberOfWorkers = 2)

    queue ! Enqueue("a", sendAcks = true)
    expectMsg(WorkEnqueued)
    delegatee.expectMsg("a")
  }

  "send results to an actor" in new QueueScope {
    val queue = defaultQueue()
    val workerPoolManager = initQueue(queue, numberOfWorkers = 2)

    val sendProbe = TestProbe()

    queue ! Enqueue("a", sendResultsTo = Some(sendProbe.ref))

    delegatee.expectMsg("a")
    delegatee.reply(MessageProcessed("response"))
    sendProbe.expectMsg("response")
  }

  "reject work when retiring" in new QueueScope {
    val queue = defaultQueue()
    watch(queue)
    val workerPoolManager = initQueue(queue, numberOfWorkers = 1)
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
        GeneralActorRefHandler("tst", TestProbe().ref, system)(resultChecker),
        TestProbe().ref
      )

      val queue: QueueRef = defaultQueue(WorkSettings(timeout = 60.milliseconds))
      val workerPool: ActorRef = TestActorRef(defaultWorkerPoolProps(queue, metricsCollector = metricsCollector))

      watch(workerPool)

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

  val metricsCollector: ActorRef = system.actorOf(QueueSampler.props(None)) // To be overridden

  def initQueue(queue: ActorRef, numberOfWorkers: Int = 1, minPoolSize: Int = 1): WorkerPoolManagerRef = {
    val workerPoolProps: Props = defaultWorkerPoolProps(queue, ProcessingWorkerPoolSettings(startingPoolSize = numberOfWorkers, minPoolSize = minPoolSize), metricsCollector)
    system.actorOf(workerPoolProps)
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

  override val metricsCollector: ActorRef = system.actorOf(QueueSampler.props(Some(new Reporter {
    def report(metric: Metric): Unit = receivedMetrics = metric :: receivedMetrics
  })))

}

