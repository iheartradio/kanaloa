package kanaloa.reactive.dispatcher.queue

import akka.actor.PoisonPill
import akka.testkit.{TestActorRef, TestProbe}
import kanaloa.reactive.dispatcher.ApiProtocol.WorkFailed
import kanaloa.reactive.dispatcher.queue.Queue.Unregistered
import kanaloa.reactive.dispatcher.queue.Worker.Retire

class WorkerUnregisteringBusySpec extends WorkerSpec {

  def withUnregisteringBusyWorker(test: (TestActorRef[Worker], TestProbe, TestProbe, Work) ⇒ Any) {
    withWorkingWorker(WorkSettings()) { (worker, queueProbe, routeeProbe, work) ⇒
      worker ! Retire //this changes the Worker's state into 'unregisteringbusy'
      test(worker, queueProbe, routeeProbe, work)
    }
  }

  "An UnregisteringBusy Worker" should {

    "status is UnregisteringBusy" in withUnregisteringBusyWorker { (worker, queueProbe, routeeProbe, work) ⇒
      assertWorkerStatus(worker, Worker.UnregisteringBusy)
    }

    "reject Work" in withUnregisteringBusyWorker { (worker, queueProbe, routeeProbe, work) ⇒
      val w = Work("moreWork")
      worker ! w
      expectMsg(Rejected(w, "Retiring"))
    }

    "transition to 'unregisteringIdle' if WorkComplete" in withUnregisteringBusyWorker { (worker, queueProbe, routeeProbe, work) ⇒
      routeeProbe.send(worker, Result("finished!"))
      expectMsg("finished!")
      assertWorkerStatus(worker, Worker.UnregisteringIdle)
    }

    "transition to 'unregisteringIdle' Routee dies" in withUnregisteringBusyWorker { (worker, queueProbe, routeeProbe, work) ⇒
      routeeProbe.ref ! PoisonPill
      expectMsgType[WorkFailed]
      assertWorkerStatus(worker, Worker.UnregisteringIdle)
    }

    "transition to 'unregisteringIdle' if Work fails" in withUnregisteringBusyWorker { (worker, queueProbe, routeeProbe, work) ⇒
      routeeProbe.send(worker, Fail("finished!"))
      expectMsgType[WorkFailed]
      assertWorkerStatus(worker, Worker.UnregisteringIdle)
    }

    "transition to 'waitingToTerminate' if Unregistered" in withUnregisteringBusyWorker { (worker, queueProbe, routeeProbe, work) ⇒
      worker ! Unregistered
      assertWorkerStatus(worker, Worker.WaitingToTerminate)
    }

    "transition to 'waitingToTerminate if Terminated(queue)" in withUnregisteringBusyWorker { (worker, queueProbe, routeeProbe, work) ⇒
      queueProbe.ref ! PoisonPill
      assertWorkerStatus(worker, Worker.WaitingToTerminate)
    }
  }
}
