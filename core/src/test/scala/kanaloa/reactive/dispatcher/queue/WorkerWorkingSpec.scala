package kanaloa.reactive.dispatcher.queue

import akka.actor.PoisonPill
import kanaloa.reactive.dispatcher.ApiProtocol.{WorkFailed, WorkTimedOut}
import kanaloa.reactive.dispatcher.metrics.Metric
import kanaloa.reactive.dispatcher.queue.Queue.{NoWorkLeft, RequestWork, Unregister}
import kanaloa.reactive.dispatcher.queue.Worker.{SetDelay, UnregisteringIdle}

import scala.concurrent.duration._

class WorkerWorkingSpec extends WorkerSpec {

  "A Working Worker" should {
    "status is Working" in withWorkingWorker() { (worker, queueProbe, routeeProbe, work, _) ⇒
      assertWorkerStatus(worker, Worker.Working)
    }

    "reject Work" in withWorkingWorker() { (worker, queueProbe, routeeProbe, work, _) ⇒
      val work = Work("moreWork")
      worker ! work
      expectMsg(Rejected(work, "Busy"))
    }

    "handle successful Work, transitions to 'idle'" in withWorkingWorker() { (worker, queueProbe, routeeProbe, work, _) ⇒
      routeeProbe.send(worker, Result("Response"))
      //since we set 'self' as the replyTo, we should get the response
      expectMsg("Response")
      queueProbe.expectMsg(RequestWork(worker)) //asks for more Work now because it is idle
      assertWorkerStatus(worker, Worker.Idle)
    }

    "report successful work to metricsCollector'" in withWorkingWorker() { (worker, _, routeeProbe, _, metricCollectorProbe) ⇒
      routeeProbe.send(worker, Result("Response"))
      expectMsg("Response")
      metricCollectorProbe.expectMsgType[Metric.WorkCompleted]
    }

    "consume Hold when asking for more Work, transitions to 'idle'" in withWorkingWorker() { (worker, queueProbe, routeeProbe, work, metricCollectorProbe) ⇒
      val holdTime = 50.milliseconds
      worker ! SetDelay(Some(holdTime))
      metricCollectorProbe.expectMsg(Metric.CircuitBreakerOpened)
      eventually {
        worker.underlyingActor.delayBeforeNextWork.value shouldBe holdTime
      }
      routeeProbe.send(worker, Result("Response"))
      //since we set 'self' as the replyTo, we should get the response
      expectMsg("Response")

      queueProbe.expectNoMsg((holdTime * 0.9).asInstanceOf[FiniteDuration]) //should not get request for work within hold
      queueProbe.expectMsg(RequestWork(worker)) //asks for more Work now because it is idle
      assertWorkerStatus(worker, Worker.Idle)
    }

    "handle failed Work, transitions to 'idle'" in withWorkingWorker() { (worker, queueProbe, routeeProbe, work, _) ⇒
      routeeProbe.send(worker, Fail("sad panda :("))
      expectMsgType[WorkFailed]
      queueProbe.expectMsg(RequestWork(worker)) //asks for more Work now because it is idle
      assertWorkerStatus(worker, Worker.Idle)
    }

    "report failed work to metricsCollector'" in withWorkingWorker() { (worker, _, routeeProbe, _, metricCollectorProbe) ⇒
      routeeProbe.send(worker, Fail("sad panda :("))
      expectMsgType[WorkFailed]
      metricCollectorProbe.expectMsg(Metric.WorkFailed)
    }

    "apply retries on failed Work, transitions to 'idle'" in withWorkingWorker(WorkSettings(retry = 1)) { (worker, queueProbe, routeeProbe, work, _) ⇒
      routeeProbe.send(worker, Fail("sad panda :(")) //first fail
      routeeProbe.expectMsg(work.messageToDelegatee)
      routeeProbe.send(worker, Fail("still a sad panda :( :("))
      expectMsgType[WorkFailed]
      queueProbe.expectMsg(RequestWork(worker)) //asks for more Work now because it is idle
      assertWorkerStatus(worker, Worker.Idle)
    }

    "time out Work, transitions to 'idle'" in withWorkingWorker(WorkSettings(timeout = 5.milliseconds)) { (worker, queueProbe, routeeProbe, work, _) ⇒
      expectMsgType[WorkTimedOut]
      queueProbe.expectMsg(RequestWork(worker)) //asks for more Work now because it is idle
      assertWorkerStatus(worker, Worker.Idle)
    }

    "report timeout work to metricsCollector'" in withWorkingWorker(WorkSettings(timeout = 5.milliseconds)) { (worker, _, routeeProbe, _, metricCollectorProbe) ⇒
      expectMsgType[WorkTimedOut]
      metricCollectorProbe.expectMsg(Metric.WorkTimedOut)
    }

    "fail Work, transitions to 'unregisteringIdle' if Terminated(routee)" in withWorkingWorker() { (worker, queueProbe, routeeProbe, work, _) ⇒
      routeeProbe.ref ! PoisonPill
      expectMsgType[WorkFailed]
      queueProbe.expectMsg(Unregister(worker))
      assertWorkerStatus(worker, UnregisteringIdle)
    }

    "transition to 'waitingToTerminate' if Terminated(queue)" in withWorkingWorker() { (worker, queueProbe, _, work, _) ⇒
      queueProbe.ref ! PoisonPill
      assertWorkerStatus(worker, Worker.WaitingToTerminate)
    }

    "transition to 'waitingToTerminate' if NoWorkLeft'" in withWorkingWorker() { (worker, _, _, work, _) ⇒
      worker ! NoWorkLeft
      assertWorkerStatus(worker, Worker.WaitingToTerminate)
    }

    "transition to 'unregisteringBusy' if Retire'" in withWorkingWorker() { (worker, _, _, work, _) ⇒
      worker ! Worker.Retire
      assertWorkerStatus(worker, Worker.UnregisteringBusy)
    }
  }

  "A Working Worker with circuit breaker" should {
    val cbs = Some(CircuitBreakerSettings(openDurationBase = 50.milliseconds, timeoutCountThreshold = 2))
    val ws = WorkSettings(timeout = 5.milliseconds)
    "not trigger circuit breaker when counter is below threshold" in withWorkingWorker(ws, cbs) {
      (worker, queueProbe, _, work, _) ⇒
        //after one time out,
        queueProbe.expectMsg(40.milliseconds, RequestWork(worker))

        worker.underlyingActor.delayBeforeNextWork should be(empty)

    }

    "trigger circuit breaker when counter is above threshold" in withWorkingWorker(ws, cbs) {
      (worker, queueProbe, _, work, _) ⇒

        queueProbe.expectMsg(40.milliseconds, RequestWork(worker))

        queueProbe.send(worker, work) //second time out
        eventually {
          worker.underlyingActor.delayBeforeNextWork should contain(cbs.get.openDurationBase * 2)
        }

    }

    "reset circuit breaker when a success is received" in withWorkingWorker(ws, cbs) {
      (worker, queueProbe, routeeProbe, work, _) ⇒

        queueProbe.expectMsg(40.milliseconds, RequestWork(worker))

        queueProbe.send(worker, work) //second time out

        eventually {
          worker.underlyingActor.delayBeforeNextWork should contain(cbs.get.openDurationBase * 2)
        }

        queueProbe.expectMsg(RequestWork(worker))
        queueProbe.send(worker, work.copy(settings = WorkSettings()))
        routeeProbe.send(worker, Result("Response"))

        eventually {
          worker.underlyingActor.delayBeforeNextWork should be(empty)
        }(PatienceConfig(timeout = 400.milliseconds))

    }

    "reset circuit breaker when a regular failure  is received" in withWorkingWorker(ws, cbs) {
      (worker, queueProbe, routeeProbe, work, _) ⇒

        queueProbe.expectMsg(40.milliseconds, RequestWork(worker))

        queueProbe.send(worker, work) //second time out

        eventually {
          worker.underlyingActor.delayBeforeNextWork should contain(cbs.get.openDurationBase * 2)
        }

        queueProbe.expectMsg(RequestWork(worker))
        queueProbe.send(worker, work.copy(settings = WorkSettings()))
        routeeProbe.send(worker, Fail("sad red panda"))

        eventually {
          worker.underlyingActor.delayBeforeNextWork should be(empty)
        }(PatienceConfig(timeout = 400.milliseconds))

    }
  }
}
