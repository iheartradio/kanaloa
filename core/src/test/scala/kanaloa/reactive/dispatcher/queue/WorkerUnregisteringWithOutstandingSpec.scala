package kanaloa.reactive.dispatcher.queue

import akka.actor.PoisonPill
import akka.testkit.{TestActorRef, TestProbe}
import kanaloa.reactive.dispatcher.ApiProtocol.WorkFailed
import kanaloa.reactive.dispatcher.queue.Queue.Unregistered
import kanaloa.reactive.dispatcher.queue.Worker.Retire

class WorkerUnregisteringWithOutstandingSpec extends WorkerSpec {

  def withUnregisteringOutstandingWorker(test: (TestActorRef[Worker], TestProbe, TestProbe, Work) ⇒ Any) {
    withWorkingWorker(WorkSettings()) { (worker, queueProbe, routeeProbe, work) ⇒
      worker ! Retire //this changes the Worker's state into 'unregisteringoutstanding'
      test(worker, queueProbe, routeeProbe, work)
    }
  }

  "An UnregisteringWithOutstanding Worker" should {

    "status is UnregisteringOutstanding" in withUnregisteringOutstandingWorker { (worker, queueProbe, routeeProbe, work) ⇒
      assertWorkerStatus(worker, Worker.UnregisteringOutstanding)
    }

    "reject Work" in withUnregisteringOutstandingWorker { (worker, queueProbe, routeeProbe, work) ⇒
      val w = Work("moreWork")
      worker ! w
      expectMsg(Rejected(w, "Retiring"))
    }

    "transition to 'unregistering' if WorkComplete" in withUnregisteringOutstandingWorker { (worker, queueProbe, routeeProbe, work) ⇒
      routeeProbe.send(worker, Result("finished!"))
      expectMsg("finished!")
      assertWorkerStatus(worker, Worker.Unregistering)
    }

    "transition to 'unregistering' Routee dies" in withUnregisteringOutstandingWorker { (worker, queueProbe, routeeProbe, work) ⇒
      routeeProbe.ref ! PoisonPill
      expectMsgType[WorkFailed]
      assertWorkerStatus(worker, Worker.Unregistering)
    }

    "transition to 'unregistering' if Work fails" in withUnregisteringOutstandingWorker { (worker, queueProbe, routeeProbe, work) ⇒
      routeeProbe.send(worker, Fail("finished!"))
      expectMsgType[WorkFailed]
      assertWorkerStatus(worker, Worker.Unregistering)
    }

    "transition to 'waitingToTerminate' if Unregistered" in withUnregisteringOutstandingWorker { (worker, queueProbe, routeeProbe, work) ⇒
      worker ! Unregistered
      assertWorkerStatus(worker, Worker.WaitingToTerminate)
    }

    "transition to 'waitingToTerminate if Terminated(queue)" in withUnregisteringOutstandingWorker { (worker, queueProbe, routeeProbe, work) ⇒
      queueProbe.ref ! PoisonPill
      assertWorkerStatus(worker, Worker.WaitingToTerminate)
    }
  }
}
