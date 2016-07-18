package kanaloa.reactive.dispatcher.queue

import akka.actor.PoisonPill
import kanaloa.reactive.dispatcher.queue.Queue.{NoWorkLeft, Unregister}
import kanaloa.reactive.dispatcher.queue.Worker.Hold

import scala.concurrent.duration._

class WorkerIdleSpec extends WorkerSpec {

  "An Idle Worker" should {
    "status is Idle" in withIdleWorker { (worker, queueProbe, routeeProbe) ⇒
      assertWorkerStatus(worker, Worker.Idle)
    }

    "terminate if NoWorkLeft" in withIdleWorker { (worker, queueProbe, routeeProbe) ⇒
      worker ! NoWorkLeft
      expectTerminated(worker)
    }

    "terminate if Queue terminates" in withIdleWorker { (worker, queueProbe, routeeProbe) ⇒
      queueProbe.ref ! PoisonPill
      expectTerminated(worker)
    }

    //TODO: assert that the value was properly applied, not just set to None
    "consume Hold value when sent Work" in withIdleWorker { (worker, queueProbe, routeeProbe) ⇒
      worker ! Hold(1.millisecond)
      eventually {
        worker.underlyingActor.delayBeforeNextWork.value shouldBe 1.millisecond
      }
      worker ! Work("work")
      routeeProbe.expectMsg("work")
      worker.underlyingActor.delayBeforeNextWork shouldBe None
      assertWorkerStatus(worker, Worker.Working)
    }

    "send work to Routee and becomes Working" in withIdleWorker { (worker, queueProbe, routeeProbe) ⇒
      worker ! Work("work")
      routeeProbe.expectMsg("work")
      assertWorkerStatus(worker, Worker.Working)
    }

    "transition to 'unregisteringIdle' when sent `Retire`" in withIdleWorker { (worker, queueProbe, routeeProbe) ⇒
      worker ! Worker.Retire
      queueProbe.expectMsg(Unregister(worker))
      assertWorkerStatus(worker, Worker.UnregisteringIdle)
    }

    "transition to 'unregisteringIdle' when the Routee dies" in withIdleWorker { (worker, queueProbe, routeeProbe) ⇒
      routeeProbe.ref ! PoisonPill
      queueProbe.expectMsg(Unregister(worker))
      assertWorkerStatus(worker, Worker.UnregisteringIdle)
    }
  }

}
