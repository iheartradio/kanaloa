package kanaloa.reactive.dispatcher.queue

import akka.actor.PoisonPill
import akka.testkit.{TestActorRef, TestProbe}
import kanaloa.reactive.dispatcher.queue.Queue.Unregistered
import kanaloa.reactive.dispatcher.queue.Worker.Retire

class WorkerUnregisteringSpec extends WorkerSpec {

  def withUnregisteringWorker(test: (TestActorRef[Worker], TestProbe, TestProbe) ⇒ Any) {
    withIdleWorker { (worker, queueProbe, routeeProbe) ⇒
      worker ! Retire //this puts the worker into the unregistering state
      test(worker, queueProbe, routeeProbe)
    }
  }

  "An Unregistering Worker" should {

    "status is Unregistering" in withUnregisteringWorker { (worker, queueProbe, routeeProbe) ⇒
      assertWorkerStatus(worker, Worker.Unregistering)
    }

    "reject Work" in withUnregisteringWorker { (worker, queueProbe, routeeProbe) ⇒
      val w = Work("more work")
      worker ! w
      expectMsg(Rejected(w, "Retiring"))
    }

    "terminate when Unregister" in withUnregisteringWorker { (worker, queueProbe, routeeProbe) ⇒
      worker ! Unregistered
      expectTerminated(worker)
    }

    "terminate if Terminated(queue)" in withUnregisteringWorker { (worker, queueProbe, routeeProbe) ⇒
      queueProbe.ref ! PoisonPill
      expectTerminated(worker)
    }
  }
}
