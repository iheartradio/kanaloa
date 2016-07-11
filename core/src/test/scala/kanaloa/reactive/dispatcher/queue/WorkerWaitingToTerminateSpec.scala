package kanaloa.reactive.dispatcher.queue

import akka.actor.PoisonPill
import akka.testkit.{TestActorRef, TestProbe}
import kanaloa.reactive.dispatcher.ApiProtocol.WorkFailed
import kanaloa.reactive.dispatcher.queue.Queue.NoWorkLeft

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
