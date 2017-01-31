package kanaloa.queue

import akka.actor._
import akka.testkit.{TestActorRef, TestProbe}
import kanaloa.ApiProtocol.{WorkTimedOut, WorkRejected, ShutdownSuccessfully}
import kanaloa.handler.GeneralActorRefHandler
import kanaloa.metrics.{Metric, Reporter}
import kanaloa.queue.Queue._
import kanaloa.TestUtils._
import kanaloa.queue.Sampler.SamplerSettings
import kanaloa.queue.WorkerPoolManager.{Shutdown, _}
import kanaloa.queue.QueueTestUtils._
import kanaloa.SpecWithActorSystem
import org.scalatest.concurrent.Eventually

import scala.concurrent.duration._
import scala.util.Random
class QueueSpec extends SpecWithActorSystem {
  "Queue" should {

    "dispatch work on demand on parallel" in new QueueScope {
      val queue = defaultQueue()
      initQueue(queue, numberOfWorkers = 3)

      service.expectNoMsg(40.milliseconds)

      queue ! Enqueue("a")
      service.expectMsg("a")

      queue ! Enqueue("b")
      service.expectMsg("b")

    }

    "won't over burden" in new QueueScope {
      val queue = defaultQueue()
      initQueue(queue, numberOfWorkers = 2)

      queue ! Enqueue("a")
      service.expectMsg("a")

      queue ! Enqueue("b")
      service.expectMsg("b")

      queue ! Enqueue("c")

      service.expectNoMsg(100.milliseconds)
    }

    "reuse workers" in new QueueScope {
      val queue = defaultQueue()
      initQueue(queue, numberOfWorkers = 2)

      queue ! Enqueue("a")
      service.expectMsg("a")
      service.reply(MessageProcessed("a"))

      queue ! Enqueue("b")
      service.expectMsg("b")
      service.reply(MessageProcessed("b"))

      queue ! Enqueue("c")
      service.expectMsg("c")
    }

    "shutdown with all outstanding work done" taggedAs (shutdown) in new QueueScope {

      val queue = defaultQueue()
      val workerPoolManager = initQueue(queue, numberOfWorkers = 2)

      queue ! Enqueue("a")

      service.expectMsg("a")

      workerPoolManager ! Shutdown(Some(self))

      expectNoMsg(100.milliseconds) //shouldn't shutdown until the last work is done

      service.reply(MessageProcessed("a"))

      expectMsg(ShutdownSuccessfully)
    }
  }

  "send ack messages when turned on" in new QueueScope {
    val queue = defaultQueue()
    val workerPoolManager = initQueue(queue, numberOfWorkers = 2)

    queue ! Enqueue("a", sendAcks = true)
    expectMsg(WorkEnqueued)
    service.expectMsg("a")
  }

  "send results to an actor" in new QueueScope {
    val queue = defaultQueue()
    val workerPoolManager = initQueue(queue, numberOfWorkers = 2)

    val sendProbe = TestProbe()

    queue ! Enqueue("a", sendResultsTo = Some(sendProbe.ref))

    service.expectMsg("a")
    service.reply(MessageProcessed("response"))
    sendProbe.expectMsg("response")
  }

  "reject work when retiring" in new QueueScope {
    val queue = defaultQueue()
    watch(queue)
    val workerPoolManager = initQueue(queue, numberOfWorkers = 1)
    queue ! Enqueue("a")
    service.expectMsg("a")
    queue ! Enqueue("b")
    service.expectNoMsg()
    //"b" shoud get buffered, since there is only one worker, who is
    //has not "finished" with "a" (we didn't have the probe send back the finished message to the Worker)
    queue ! Retire(50.milliseconds) //give this some time to kill itself
    queue ! Enqueue("c")
    expectMsg(EnqueueRejected(Enqueue("c"), Queue.EnqueueRejected.Retiring))
    //after the the Retiring state is expired, the Queue goes away
    expectTerminated(queue, 75.milliseconds)

  }

  "reject all work still in queue when retiring timed out" in new QueueScope {
    val queue = defaultQueue()

    val workerPoolManager = initQueue(queue, numberOfWorkers = 1)

    queue ! Enqueue("a", sendResultsTo = Some(self))
    service.expectMsg("a")
    queue ! Enqueue("b", sendResultsTo = Some(self))
    service.expectNoMsg()
    //message "a" is received by the service,  message "b" is still in the queue

    queue ! Retire(100.milliseconds)

    expectNoMsg(80.milliseconds) //no rejection within the timeout

    expectMsgType[WorkRejected]

  }

  "reject all work when commanded to" in new QueueScope {
    val queue = defaultQueue()
    queue ! Enqueue("a", sendResultsTo = Some(self))
    queue ! Enqueue("b", sendResultsTo = Some(self))
    queue ! Enqueue("c", sendResultsTo = Some(self))
    queue ! Enqueue("d", sendResultsTo = Some(self))

    queue ! Queue.DiscardAll("discard them")
    expectMsg(WorkRejected("discard them"))
    expectMsg(WorkRejected("discard them"))
    expectMsg(WorkRejected("discard them"))
    expectMsg(WorkRejected("discard them"))
    expectNoMsg()

  }

  "abandon work directly when the work is stale" in new QueueScope {
    val queue = defaultQueue(WorkSettings(serviceTimeout = 95.milliseconds, requestTimeout = Some(100.milliseconds)))
    val workerPoolManager = initQueue(queue, numberOfWorkers = 1)

    queue ! Enqueue("a", sendResultsTo = Some(self))
    queue ! Enqueue("b", sendResultsTo = None)
    queue ! Enqueue("c", sendResultsTo = Some(self))

    service.expectMsg("a")
    expectNoMsg(60.milliseconds)
    service.reply(MessageProcessed("reply for A"))
    expectMsg("reply for A")

    queue ! Enqueue("d", sendResultsTo = Some(self))

    service.expectMsg("b")
    expectNoMsg(60.milliseconds) //120ms passed
    service.reply(MessageProcessed("replyB")) //the worker will now try to grab work c but it is stale

    service.expectMsg("d") //service gets d instead
    expectMsgType[WorkTimedOut]

    //back to normal business
    queue ! Enqueue("e", sendResultsTo = Some(self))
    service.reply(MessageProcessed("replyD"))
    expectMsg("replyD")
    service.expectMsg("e") //service gets d instead
    service.reply(MessageProcessed("replyE"))
    expectMsg("replyE")

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

      val queue: QueueRef = defaultQueue(WorkSettings(serviceTimeout = 60.milliseconds))
      val workerPool: ActorRef = TestActorRef(defaultWorkerPoolProps(queue, metricsCollector = workerPoolMetricsCollector))

      watch(workerPool)

      queue ! Enqueue("a")

      service.expectMsg("a")
      service.reply(MessageProcessed("a"))

      queue ! Enqueue("b")
      service.expectMsg("b")
      service.reply(MessageFailed)

      queue ! Enqueue("c")
      service.expectMsg("c") //timeout this one

      queue ! Enqueue("d")
      service.expectMsg("d")

      eventually {
        receivedMetrics should contain allOf (Metric.WorkFailed, Metric.WorkTimedOut)
        receivedMetrics.collect { case x: Metric.WorkCompleted ⇒ x } should have size 1
      }
    }
  }
}

class QueueScope(implicit system: ActorSystem, factories: Factories) extends ScopeWithQueue {

  lazy val queueSampler: ActorRef = system.actorOf(QueueSampler.props(None)) // To be overridden
  lazy val workerPoolMetricsCollector: ActorRef = factories.workerPoolSampler(
    factories.workerPoolSamplerFactory(queueSampler = queueSampler, settings = SamplerSettings())
  )

  def initQueue(queue: ActorRef, numberOfWorkers: Int = 1, minPoolSize: Int = 1): WorkerPoolManagerRef = {
    val workerPoolProps: Props = defaultWorkerPoolProps(
      queue,
      WorkerPoolSettings(startingPoolSize = numberOfWorkers, minPoolSize = minPoolSize),
      workerPoolMetricsCollector
    )
    system.actorOf(workerPoolProps)
  }

  def iteratorQueue(
    iterator:      Iterator[String],
    workSetting:   WorkSettings     = WorkSettings(),
    sendResultsTo: Option[ActorRef] = None
  ): QueueRef =
    system.actorOf(
      iteratorQueueProps(iterator, queueSampler, workSetting, sendResultsTo),
      "iterator-queue-" + Random.nextInt(100000)
    )

  def defaultQueue(workSetting: WorkSettings = WorkSettings()): QueueRef =
    system.actorOf(
      Queue.default(queueSampler, workSetting),
      "default-queue-" + Random.nextInt(100000)
    )

}

class MetricCollectorScope(implicit system: ActorSystem, factories: Factories) extends QueueScope {
  @volatile
  var receivedMetrics: List[Metric] = Nil

  def mockReporter = new Reporter {
    def report(metric: Metric): Unit = receivedMetrics = metric :: receivedMetrics

    def withNewPrefix(modifier: (String) ⇒ String): Reporter = this
  }

  override lazy val queueSampler: ActorRef = system.actorOf(QueueSampler.props(Some(mockReporter)))

  override lazy val workerPoolMetricsCollector: ActorRef =
    factories.workerPoolSampler(
      factories.workerPoolSamplerFactory(queueSampler = queueSampler, settings = SamplerSettings()),
      Some(mockReporter)
    )
}

