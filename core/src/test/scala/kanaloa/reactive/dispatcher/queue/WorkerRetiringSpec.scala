package kanaloa.reactive.dispatcher.queue

import akka.actor.PoisonPill
import akka.testkit.{TestActorRef, TestProbe}
import kanaloa.reactive.dispatcher.ApiProtocol.WorkFailed
import kanaloa.reactive.dispatcher.queue.Queue.{NoWorkLeft, Unregistered}
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

class WorkerWaitingToTerminateSpec extends WorkerSpec {

  def withTerminatingWorker(settings: WorkSettings = WorkSettings())(test: (TestActorRef[Worker], TestProbe, TestProbe, Work) ⇒ Any) {
    withWorkingWorker(settings) { (worker, queueProbe, routeeProbe, work) ⇒
      worker ! NoWorkLeft //this changes the Worker's state into 'WaitingToTerminate
      test(worker, queueProbe, routeeProbe, work)
    }
  }

  "A Terminating Worker" should {
    "status is WaitingToTerminate" in withTerminatingWorker() { (worker, queueProbe, routeeProbe, work) ⇒
      assertWorkerStatus(worker, Worker.WaitingToTerminate)
    }

    "reject Work" in withTerminatingWorker() { (worker, queueProbe, routeeProbe, work) ⇒
      val w = Work("more work")
      worker ! w
      expectMsg(Rejected(w, "Retiring"))
    }

    "terminate if the Routee dies" in withTerminatingWorker() { (worker, queueProbe, routeeProbe, work) ⇒
      routeeProbe.ref ! PoisonPill
      expectMsgType[WorkFailed]
      expectTerminated(worker)
    }

    "terminate when the Work completes" in withTerminatingWorker() { (worker, queueProbe, routeeProbe, work) ⇒
      routeeProbe.send(worker, Result("finished!"))
      expectMsg("finished!")
      expectTerminated(worker)
    }

    "terminate if the Work fails" in withTerminatingWorker() { (worker, queueProbe, routeeProbe, work) ⇒
      routeeProbe.send(worker, Fail("fail"))
      expectMsgType[WorkFailed]
      expectTerminated(worker)
    }
  }
}
