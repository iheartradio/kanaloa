package kanaloa.queue

import akka.actor.PoisonPill
import kanaloa.queue.Queue.{NoWorkLeft, Unregister}

class WorkerIdleSpec extends WorkerSpec {

  "An Idle Worker" should {
    "status is Idle" in withIdleWorker() { (worker, _, _, _) ⇒
      assertWorkerStatus(worker, Worker.Idle)
    }

    "terminate if NoWorkLeft" in withIdleWorker() { (worker, _, _, _) ⇒
      worker ! NoWorkLeft
      expectTerminated(worker)
    }

    "terminate if Queue terminates" in withIdleWorker() { (worker, queueProbe, _, _) ⇒
      queueProbe.ref ! PoisonPill
      expectTerminated(worker)
    }

    "send work to Routee and becomes Working" in withIdleWorker() { (worker, _, routeeProbe, _) ⇒
      worker ! Work("work")
      routeeProbe.expectMsg("work")
      assertWorkerStatus(worker, Worker.Working)
    }

    "transition to 'unregisteringIdle' when sent `Retire`" in withIdleWorker() { (worker, queueProbe, _, _) ⇒
      worker ! Worker.Retire
      queueProbe.expectMsg(Unregister(worker))
      assertWorkerStatus(worker, Worker.UnregisteringIdle)
    }

    "transition to 'unregisteringIdle' when the Routee dies" in withIdleWorker() { (worker, queueProbe, routeeProbe, _) ⇒
      routeeProbe.ref ! PoisonPill
      queueProbe.expectMsg(Unregister(worker))
      assertWorkerStatus(worker, Worker.UnregisteringIdle)
    }
  }

}
