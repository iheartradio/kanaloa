package kanaloa.reactive.dispatcher.queue

import kanaloa.reactive.dispatcher.ApiProtocol.ShutdownSuccessfully
import kanaloa.reactive.dispatcher.{SpecWithActorSystem, Backends}
import kanaloa.reactive.dispatcher.queue.QueueProcessor.Shutdown
import kanaloa.reactive.dispatcher.queue.TestUtils._
import org.scalatest.concurrent.Eventually
import org.scalatest.mock.MockitoSugar

import concurrent.duration._

class IteratorQueueSpec extends SpecWithActorSystem {
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
}
