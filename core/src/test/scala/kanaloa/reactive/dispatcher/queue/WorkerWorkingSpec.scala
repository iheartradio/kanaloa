package kanaloa.reactive.dispatcher.queue

import akka.actor.PoisonPill
import kanaloa.reactive.dispatcher.ApiProtocol.{WorkFailed, WorkTimedOut}
import kanaloa.reactive.dispatcher.queue.Queue.{NoWorkLeft, RequestWork, Unregister}
import kanaloa.reactive.dispatcher.queue.Worker.{Hold, Unregistering}

import scala.concurrent.duration._

class WorkerWorkingSpec extends WorkerSpec {

  "A Working Worker" should {
    "status is Working" in withWorkingWorker() { (worker, queueProbe, routeeProbe, work) ⇒
      assertWorkerStatus(worker, Worker.Working)
    }

    "reject Work" in withWorkingWorker() { (worker, queueProbe, routeeProbe, work) ⇒
      val work = Work("moreWork")
      worker ! work
      expectMsg(Rejected(work, "Busy"))
    }

    "handle successful Work, transitions to 'idle'" in withWorkingWorker() { (worker, queueProbe, routeeProbe, work) ⇒
      routeeProbe.send(worker, Result("Response"))
      //since we set 'self' as the replyTo, we should get the response
      expectMsg("Response")
      queueProbe.expectMsg(RequestWork(worker)) //asks for more Work now because it is idle
      assertWorkerStatus(worker, Worker.Idle)
    }

    //TODO: assert that the value was properly applied, not just set to None
    "consume Hold when asking for more Work, transitions to 'idle'" in withWorkingWorker() { (worker, queueProbe, routeeProbe, work) ⇒
      worker ! Hold(1.millisecond)
      eventually {
        worker.underlyingActor.delayBeforeNextWork.value shouldBe 1.millisecond
      }
      routeeProbe.send(worker, Result("Response"))
      //since we set 'self' as the replyTo, we should get the response
      expectMsg("Response")
      queueProbe.expectMsg(RequestWork(worker)) //asks for more Work now because it is idle
      worker.underlyingActor.delayBeforeNextWork shouldBe None
      assertWorkerStatus(worker, Worker.Idle)
    }

    "handle failed Work, transitions to 'idle'" in withWorkingWorker() { (worker, queueProbe, routeeProbe, work) ⇒
      routeeProbe.send(worker, Fail("sad panda :("))
      expectMsgType[WorkFailed]
      queueProbe.expectMsg(RequestWork(worker)) //asks for more Work now because it is idle
      assertWorkerStatus(worker, Worker.Idle)
    }

    "apply retries on failed Work, transitions to 'idle'" in withWorkingWorker(WorkSettings(retry = 1)) { (worker, queueProbe, routeeProbe, work) ⇒
      routeeProbe.send(worker, Fail("sad panda :(")) //first fail
      routeeProbe.expectMsg(work.messageToDelegatee)
      routeeProbe.send(worker, Fail("still a sad panda :( :("))
      expectMsgType[WorkFailed]
      queueProbe.expectMsg(RequestWork(worker)) //asks for more Work now because it is idle
      assertWorkerStatus(worker, Worker.Idle)
    }

    "time out Work, transitions to 'idle'" in withWorkingWorker(WorkSettings(timeout = 5.milliseconds)) { (worker, queueProbe, routeeProbe, work) ⇒
      expectMsgType[WorkTimedOut]
      queueProbe.expectMsg(RequestWork(worker)) //asks for more Work now because it is idle
      assertWorkerStatus(worker, Worker.Idle)
    }

    "fail Work, transitions to 'unregistering' if Terminated(routee)" in withWorkingWorker() { (worker, queueProbe, routeeProbe, work) ⇒
      routeeProbe.ref ! PoisonPill
      expectMsgType[WorkFailed]
      queueProbe.expectMsg(Unregister(worker))
      assertWorkerStatus(worker, Unregistering)
    }

    "transition to 'waitingToTerminate' if Terminated(queue)" in withWorkingWorker() { (worker, queueProbe, routeeProbe, work) ⇒
      queueProbe.ref ! PoisonPill
      assertWorkerStatus(worker, Worker.WaitingToTerminate)
    }

    "transition to 'waitingToTerminate' if NoWorkLeft'" in withWorkingWorker() { (worker, queueProbe, routeeProbe, work) ⇒
      worker ! NoWorkLeft
      assertWorkerStatus(worker, Worker.WaitingToTerminate)
    }

    "transition to 'unregisteringOutstanding' if Retire'" in withWorkingWorker() { (worker, queueProbe, routeeProbe, work) ⇒
      worker ! Worker.Retire
      assertWorkerStatus(worker, Worker.UnregisteringOutstanding)
    }
  }
}
