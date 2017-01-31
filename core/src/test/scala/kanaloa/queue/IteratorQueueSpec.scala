package kanaloa.queue

import kanaloa.ApiProtocol.ShutdownSuccessfully
import kanaloa.SpecWithActorSystem
import kanaloa.TestUtils.MockActors
import kanaloa.queue.WorkerPoolManager.Shutdown
import kanaloa.queue.QueueTestUtils._
import org.scalatest.concurrent.Eventually
import org.scalatest.mock.MockitoSugar

import concurrent.duration._

class IteratorQueueSpec extends SpecWithActorSystem {
  "Iterator Queue" should {
    "Process through a list of tasks in sequence with one worker" in new QueueScope {
      val workerPoolManager = initQueue(iteratorQueue(List("a", "b", "c").iterator))

      service.expectMsg(DelegateeMessage("a"))
      service.reply(MessageProcessed("a"))
      service.expectMsg(DelegateeMessage("b"))
      service.reply(MessageProcessed("b"))
      service.expectMsg(DelegateeMessage("c"))
      service.reply(MessageProcessed("c"))
    }

    "shutdown with all outstanding work done from the queue side" in new QueueScope {
      val workerPoolManager = initQueue(iteratorQueue(List("a", "b", "c", "d").iterator, sendResultsTo = Some(self)))

      service.expectMsg(DelegateeMessage("a"))
      service.reply(MessageProcessed("a"))
      service.expectMsg(DelegateeMessage("b"))

      workerPoolManager ! Shutdown(Some(self))

      expectMsg("a")
      expectNoMsg(100.milliseconds) //shouldn't shutdown until the last work is done

      service.reply(MessageProcessed("b"))

      expectMsg("b")

      expectMsg(ShutdownSuccessfully)

    }

    "abandon work when delegatee times out" in new QueueScope {
      val workerPoolManager = initQueue(iteratorQueue(List("a", "b").iterator, WorkSettings(serviceTimeout = 288.milliseconds)))

      service.expectMsg(DelegateeMessage("a"))

      service.expectNoMsg(250.milliseconds)

      service.expectMsg(DelegateeMessage("b"))
      watch(workerPoolManager)

      workerPoolManager ! Shutdown()

      expectTerminated(workerPoolManager)
    }

    "does not retrieve work without workers" in new QueueScope with MockActors with MockitoSugar with Eventually {
      import org.mockito.Mockito._
      val iterator = mock[Iterator[String]]
      val queue = system.actorOf(Queue.ofIterator(iterator, queueSampler, WorkSettings(), Some(self)))

      expectNoMsg(40.milliseconds)
      verifyNoMoreInteractions(iterator)

    }

  }
}
