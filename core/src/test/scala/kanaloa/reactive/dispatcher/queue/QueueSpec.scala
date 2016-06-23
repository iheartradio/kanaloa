package kanaloa.reactive.dispatcher.queue

import akka.actor._
import akka.testkit.{TestActorRef, TestProbe}
import kanaloa.reactive.dispatcher.ApiProtocol.{QueryStatus, ShutdownSuccessfully, WorkRejected}
import kanaloa.reactive.dispatcher.metrics.Metric.ProcessTime
import kanaloa.reactive.dispatcher.metrics.{Metric, MetricsCollector, NoOpMetricsCollector}
import kanaloa.reactive.dispatcher.queue.Queue._
import kanaloa.reactive.dispatcher.queue.QueueProcessor.{Shutdown, _}
import kanaloa.reactive.dispatcher.queue.TestUtils._
import kanaloa.reactive.dispatcher.SpecWithActorSystem

import scala.concurrent.duration._
import scala.util.Random

class QueueSpec extends SpecWithActorSystem {

  "Happy Path of iterator queue" should {
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
      val queueProcessor = initQueue(iteratorQueue(List("a", "b", "c", "d").iterator))

      delegatee.expectMsg(DelegateeMessage("a"))
      delegatee.reply(MessageProcessed("a"))
      delegatee.expectMsg(DelegateeMessage("b"))

      queueProcessor ! Shutdown(Some(self))

      expectNoMsg(100.milliseconds) //shouldn't shutdown until the last work is done

      delegatee.reply(MessageProcessed("b"))

      delegatee.expectMsg(DelegateeMessage("c")) // c is already placed in buffer
      delegatee.reply(MessageProcessed("c"))

      expectMsg(ShutdownSuccessfully)

    }

    "shutdown with all outstanding work done from the workers side" in new QueueScope {
      val queueProcessor = initQueue(iteratorQueue(List("a", "b", "c", "d").iterator))

      delegatee.expectMsg(DelegateeMessage("a"))
      delegatee.reply(MessageProcessed("a"))
      delegatee.expectMsg(DelegateeMessage("b"))

      queueProcessor ! Shutdown(Some(self), retireQueue = false)

      expectNoMsg(100.milliseconds) //shouldn't shutdown until the last work is done

      delegatee.reply(MessageProcessed("b"))

      delegatee.expectNoMsg(50.milliseconds) //although c is still in queue's buffer worker already retired.
      expectMsg(ShutdownSuccessfully)

    }

  }

  "Sad path" should {

    "abandon work when delegatee times out" in new QueueScope {
      val queueProcessor = initQueue(iteratorQueue(List("a", "b").iterator, WorkSettings(timeout = 288.milliseconds)))

      delegatee.expectMsg(DelegateeMessage("a"))

      delegatee.expectNoMsg(250.milliseconds)

      delegatee.expectMsg(DelegateeMessage("b"))

      queueProcessor ! Shutdown
    }

  }
}

class ScalingWhenWorkingSpec extends SpecWithActorSystem {

  "scaling" should {

    "send PoolSize metric when pool size changes" in new MetricCollectorScope {

      val queueProcessor = initQueue(
        iteratorQueue(Iterator("a", "b")), //make sure queue remains alive during test
        numberOfWorkers = 1
      )
      queueProcessor ! ScaleTo(3)
      queueProcessor ! ScaleTo(5)

      expectNoMsg(200.milliseconds) //wait

      receivedMetrics should contain allOf (Metric.PoolSize(1), Metric.PoolSize(3), Metric.PoolSize(5))
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

class CircuitBreakerSpec extends SpecWithActorSystem {

  "Circuit Breaker" should {

    "worker cools down after consecutive errors" in new QueueScope {
      val queue = defaultQueue()

      system.actorOf(queueProcessorWithCBProps(
        queue,
        CircuitBreakerSettings(historyLength = 3, closeDuration = 500.milliseconds)
      ))

      queue ! Enqueue("a")
      delegatee.expectMsg("a")
      delegatee.reply(MessageProcessed("a"))

      queue ! Enqueue("b")
      delegatee.expectMsg("b")
      delegatee.reply(MessageFailed)

      queue ! Enqueue("c")
      delegatee.expectMsg("c")
      delegatee.reply(MessageFailed)

      queue ! Enqueue("d")
      delegatee.expectMsg("d")
      delegatee.reply(MessageFailed)

      delegatee.expectNoMsg(70.milliseconds) //give some time for the circuit breaker to kick in

      queue ! Enqueue("e")
      delegatee.expectNoMsg(150.milliseconds)

      delegatee.expectMsg("e")

    }

    "worker report to metrics after consecutive errors" in new MetricCollectorScope() {
      val queue = defaultQueue()
      system.actorOf(queueProcessorWithCBProps(
        queue,
        CircuitBreakerSettings(historyLength = 2, closeDuration = 300.milliseconds)
      ))

      queue ! Enqueue("a")
      queue ! Enqueue("b")
      delegatee.expectMsg("a")
      delegatee.reply(MessageFailed)
      delegatee.expectMsg("b")
      delegatee.reply(MessageFailed)

      delegatee.expectNoMsg(30.milliseconds) //give some time for the circuit breaker to kick in

      receivedMetrics should contain(Metric.CircuitBreakerOpened)

    }
  }
}

class DefaultQueueSpec extends SpecWithActorSystem {
  "DefaultQueue" should {
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
}

class QueueMetricsSpec extends SpecWithActorSystem {

  "Queue Metrics" should {

    "send metric on Enqueue" in new MetricCollectorScope {

      val queue = defaultQueue()
      initQueue(queue, numberOfWorkers = 1)

      queue ! Enqueue("a")

      queue ! Enqueue("b")

      expectNoMsg(100.milliseconds) //wait

      receivedMetrics should contain allOf (Metric.WorkQueueLength(0), Metric.WorkQueueLength(1))

    }

    "send metric on failed Enqueue" in new MetricCollectorScope {

      val queue = withBackPressure(BackPressureSettings(maxBufferSize = 1))

      queue ! Enqueue("a")

      queue ! Enqueue("b")
      expectMsg(EnqueueRejected("b", Queue.EnqueueRejected.OverCapacity, None))

      queue ! Enqueue("c")
      expectMsg(EnqueueRejected("c", Queue.EnqueueRejected.OverCapacity, None))

      receivedMetrics should contain(Metric.WorkQueueLength(0))

      //TODO: we might want to make some of our own matchers for lists, the predefined ones in the DSL
      receivedMetrics.filter(_ == Metric.EnqueueRejected) should have size 2
    }

    "send WorkCompleted, ProcessTime, WorkFailed, and WorkTimedOut metrics" in new MetricCollectorScope() {

      val workerProps: Props = Worker.default(
        TestProbe().ref,
        Props.empty
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

      receivedMetrics should contain allOf (Metric.WorkCompleted, Metric.WorkFailed, Metric.WorkTimedOut)
      receivedMetrics.collect { case x: ProcessTime ⇒ x } should have size 1
    }
  }
}

class QueueScope(implicit system: ActorSystem) extends ScopeWithQueue {
  val metricsCollector: MetricsCollector = NoOpMetricsCollector // To be overridden

  def queueProcessorWithCBProps(queue: QueueRef, circuitBreakerSettings: CircuitBreakerSettings) =
    QueueProcessor.withCircuitBreaker(queue, backend, ProcessingWorkerPoolSettings(startingPoolSize = 1), circuitBreakerSettings, metricsCollector) {
      case MessageProcessed(msg) ⇒ Right(msg)
      case MessageFailed         ⇒ Left("doesn't matter")
    }

  def initQueue(queue: ActorRef, numberOfWorkers: Int = 1, minPoolSize: Int = 1): QueueProcessorRef = {
    val processorProps: Props = defaultProcessorProps(queue, ProcessingWorkerPoolSettings(startingPoolSize = numberOfWorkers, minPoolSize = minPoolSize), metricsCollector)
    system.actorOf(processorProps)
  }

  def waitForWorkerRegistration(queue: QueueRef, numberOfWorkers: Int): Unit = {
    queue ! QueryStatus()
    fishForMessage(500.millisecond, "wait for workers to register") {
      case qs: QueueStatus ⇒
        val registered = qs.queuedWorkers.size == numberOfWorkers
        if (!registered) queue ! QueryStatus()
        registered
    }
  }

  def iteratorQueue(iterator: Iterator[String], workSetting: WorkSettings = WorkSettings(), sendResultsTo: Option[ActorRef] = None, historySettings: DispatchHistorySettings = DispatchHistorySettings()): QueueRef =
    system.actorOf(
      iteratorQueueProps(iterator, historySettings, workSetting, sendResultsTo, metricsCollector),
      "iterator-queue-" + Random.nextInt(100000)
    )

  def defaultQueue(workSetting: WorkSettings = WorkSettings(), historySettings: DispatchHistorySettings = DispatchHistorySettings()): QueueRef =
    system.actorOf(
      Queue.default(historySettings, workSetting, metricsCollector),
      "default-queue-" + Random.nextInt(100000)
    )

  def withBackPressure(
    backPressureSetting: BackPressureSettings    = BackPressureSettings(),
    defaultWorkSetting:  WorkSettings            = WorkSettings(),
    historySettings:     DispatchHistorySettings = DispatchHistorySettings()
  ) = system.actorOf(
    Queue.withBackPressure(historySettings, backPressureSetting, defaultWorkSetting, metricsCollector),
    "with-back-pressure-queue" + Random.nextInt(500000)
  )
}

class MetricCollectorScope(implicit system: ActorSystem) extends QueueScope {
  @volatile
  var receivedMetrics: List[Metric] = Nil

  override val metricsCollector: MetricsCollector = new MetricsCollector {
    def send(metric: Metric): Unit = receivedMetrics = metric :: receivedMetrics
  }

}

