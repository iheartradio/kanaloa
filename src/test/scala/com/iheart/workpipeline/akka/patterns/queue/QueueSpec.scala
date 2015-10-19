package com.iheart.workpipeline.akka.patterns.queue


import akka.actor._
import com.iheart.workpipeline.akka.{SpecWithActorSystem, patterns}
import com.iheart.workpipeline.akka.patterns.CommonProtocol.{ShutdownSuccessfully, QueryStatus}
import com.iheart.workpipeline.metrics.{Metric, MetricsCollector, NoOpMetricsCollector}
import Queue._
import QueueProcessor._
import com.iheart.workpipeline.akka.patterns.queue.Queue.{QueueStatus, WorkEnqueued}
import com.iheart.workpipeline.akka.patterns.queue.QueueProcessor.{Shutdown}
import scala.concurrent.duration._
import scala.util.Random
import org.specs2.mock.Mockito

import TestUtils._

class QueueSpec extends SpecWithActorSystem {

  "Happy Path of iterator queue" >> {
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

  "Sad path" >> {

    "abandon work when delegatee times out" in new QueueScope {
      val queueProcessor = initQueue(iteratorQueue(List("a", "b").iterator, WorkSettings(timeout = 288.milliseconds)))

      delegatee.expectMsg(DelegateeMessage("a"))

      delegatee.expectNoMsg(250.milliseconds)

      delegatee.expectMsg(DelegateeMessage("b"))


      queueProcessor ! Shutdown
    }


  }
}

class ScalingWhenWorkingSpec extends SpecWithActorSystem with Mockito {

  "send PoolSize metric when pool size changes" in new QueueScope {
    override val metricsCollector = mock[MetricsCollector]

    val queueProcessor = initQueue(
      iteratorQueue(Iterator.empty),
      numberOfWorkers = 1)
    queueProcessor ! ScaleTo(3)
    queueProcessor ! ScaleTo(5)

    there.was(one(metricsCollector).send(Metric.PoolSize(1)))
      .andThen(one(metricsCollector).send(Metric.PoolSize(3)))
      .andThen(one(metricsCollector).send(Metric.PoolSize(5)))
  }

  "retiring a worker when there is no work" in new QueueScope {
    val queueProcessor = initQueue(iteratorQueue(List("a", "b", "c").iterator,
      WorkSettings(sendResultTo = Some(self))),
      numberOfWorkers = 2)
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
    val queueProcessor = initQueue( iteratorQueue(List("a", "b", "c").iterator,
                                    WorkSettings(sendResultTo = Some(self))),
                                    numberOfWorkers = 2)
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

class CircuitBreakerSpec extends SpecWithActorSystem {

  "Circuit Breaker" >> {

    "worker cools down after consecutive errors" in new QueueScope {

      system.actorOf(queueProcessorWithCBProps(iteratorQueue(List("a", "b", "c", "d", "e").iterator),
        CircuitBreakerSettings(historyLength = 3, closeDuration = 200.milliseconds)
      ), "queuewithCB")

      delegatee.expectMsg(DelegateeMessage("a"))
      delegatee.reply(MessageProcessed("a"))
      delegatee.expectMsg(DelegateeMessage("b"))
      delegatee.reply(MessageFailed)
      delegatee.expectMsg(DelegateeMessage("c"))
      delegatee.reply(MessageFailed)
      delegatee.expectMsg(DelegateeMessage("d"))
      delegatee.reply(MessageFailed)

      delegatee.expectNoMsg(190.milliseconds)

      delegatee.expectMsg(DelegateeMessage("e"))

    }
  }
}


class DefaultQueueSpec extends SpecWithActorSystem {

    "dispatch work on demand on parallel" in new QueueScope {
      val queue = defaultQueue()
      initQueue(queue, numberOfWorkers = 3)

      delegatee.expectNoMsg(40.milliseconds)

      queue ! Enqueue("a", replyTo = Some(self))

      expectMsg(WorkEnqueued)

      delegatee.expectMsg("a")

      queue ! Enqueue("b", Some(self))

      expectMsg(WorkEnqueued)

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

class QueueScope(implicit system: ActorSystem) extends ScopeWithQueue {
  val metricsCollector: MetricsCollector = NoOpMetricsCollector // To be overridden

  def queueProcessorWithCBProps(queue: QueueRef, circuitBreakerSettings: CircuitBreakerSettings) =
    QueueProcessor.withCircuitBreaker(queue, delegateeProps, ProcessingWorkerPoolSettings(startingPoolSize = 1), circuitBreakerSettings, metricsCollector) {
      case MessageProcessed(msg) => Right(msg)
      case MessageFailed => Left("doesn't matter")
    }


  def initQueue(queue: ActorRef, numberOfWorkers: Int = 1, minPoolSize: Int = 1) : QueueProcessorRef = {
    val processorProps: Props = defaultProcessorProps(queue, ProcessingWorkerPoolSettings(startingPoolSize = numberOfWorkers, minPoolSize = minPoolSize), metricsCollector)
    system.actorOf(processorProps)
  }

  def waitForWorkerRegistration(queue: QueueRef, numberOfWorkers: Int): Unit = {
    queue ! QueryStatus()
    fishForMessage(500.millisecond, "wait for workers to register"){
      case qs : QueueStatus =>
        val registered = qs.queuedWorkers.size == numberOfWorkers
        if(!registered) queue ! QueryStatus()
        registered
    }
  }
  
  def iteratorQueue(iterator: Iterator[String], workSetting: WorkSettings = WorkSettings()): QueueRef =
    system.actorOf(iteratorQueueProps(iterator, workSetting), "iterator-queue-" + Random.nextInt(100000))

  def defaultQueue(workSetting: WorkSettings = WorkSettings()): QueueRef =
    system.actorOf(Queue.default(workSetting), "default-queue-" + Random.nextInt(100000))


  def withBackPressure(backPressureSetting: BackPressureSettings = BackPressureSettings(),
                        defaultWorkSetting: WorkSettings = WorkSettings()) =
      system.actorOf(Queue.withBackPressure(backPressureSetting, defaultWorkSetting), "with-back-pressure-queue" + Random.nextInt(500000))
}

