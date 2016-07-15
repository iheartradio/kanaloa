package kanaloa.reactive.dispatcher.queue

import akka.actor.PoisonPill
import akka.testkit.{TestActorRef, TestProbe}
import kanaloa.reactive.dispatcher.queue.Queue.Unregistered
import kanaloa.reactive.dispatcher.queue.Worker.Retire

class WorkerUnregisteringIdleSpec extends WorkerSpec {

  def withUnregisteringIdleWorker(test: (TestActorRef[Worker], TestProbe, TestProbe) ⇒ Any) {
    withIdleWorker { (worker, queueProbe, routeeProbe) ⇒
      worker ! Retire //this puts the worker into the unregistering state
      test(worker, queueProbe, routeeProbe)
    }
  }

  "An Unregistering idle Worker" should {

    "status is UnregisteringIdle" in withUnregisteringIdleWorker { (worker, queueProbe, routeeProbe) ⇒
      assertWorkerStatus(worker, Worker.UnregisteringIdle)
    }

    "reject Work" in withUnregisteringIdleWorker { (worker, queueProbe, routeeProbe) ⇒
      val w = Work("more work")
      worker ! w
      expectMsg(Rejected(w, "Retiring"))
    }

    "terminate when Unregister" in withUnregisteringIdleWorker { (worker, queueProbe, routeeProbe) ⇒
      worker ! Unregistered
      expectTerminated(worker)
    }

    "terminate if Terminated(queue)" in withUnregisteringIdleWorker { (worker, queueProbe, routeeProbe) ⇒
      queueProbe.ref ! PoisonPill
      expectTerminated(worker)
    }
  }
}
