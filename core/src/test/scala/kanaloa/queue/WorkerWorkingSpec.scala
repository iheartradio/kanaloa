package kanaloa.queue

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import akka.testkit.{TestActorRef, TestProbe}
import kanaloa.ApiProtocol.{WorkFailed, WorkTimedOut}
import kanaloa.handler.{GeneralActorRefHandler, Handler}
import kanaloa.metrics.Metric
import kanaloa.queue.Queue.{NoWorkLeft, RequestWork, Unregister}
import kanaloa.queue.Worker.UnregisteringIdle

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
      routeeProbe.reply(Result("Response"))
      //since we set 'self' as the replyTo, we should get the response
      expectMsg("Response")
      queueProbe.expectMsg(RequestWork(worker)) //asks for more Work now because it is idle
      assertWorkerStatus(worker, Worker.Idle)
    }

    "report successful work to metricsCollector'" in withWorkingWorker() { (worker, _, routeeProbe, _, metricCollectorProbe) ⇒
      routeeProbe.reply(Result("Response"))
      expectMsg("Response")
      metricCollectorProbe.expectMsgType[Metric.WorkCompleted]
    }

    "handle failed Work, transitions to 'idle'" in withWorkingWorker() { (worker, queueProbe, routeeProbe, work, _) ⇒
      routeeProbe.reply(Fail("sad panda :("))
      expectMsgType[WorkFailed]
      queueProbe.expectMsg(RequestWork(worker)) //asks for more Work now because it is idle
      assertWorkerStatus(worker, Worker.Idle)
    }

    "report failed work to metricsCollector'" in withWorkingWorker() { (worker, _, routeeProbe, _, metricCollectorProbe) ⇒
      routeeProbe.reply(Fail("sad panda :("))
      expectMsgType[WorkFailed]
      metricCollectorProbe.expectMsg(Metric.WorkFailed)
    }

    "apply retries on failed Work, transitions to 'idle'" in withWorkingWorker(WorkSettings(retry = 1)) { (worker, queueProbe, routeeProbe, work, _) ⇒
      routeeProbe.reply(Fail("sad panda :(")) //first fail
      routeeProbe.expectMsg(work.messageToDelegatee)
      routeeProbe.reply(Fail("still a sad panda :( :("))
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

    "transition to 'waitingToTerminate' if Terminated(queue)" in withWorkingWorker() { (worker, queueProbe, _, work, _) ⇒
      queueProbe.ref ! PoisonPill
      assertWorkerStatus(worker, Worker.WaitingToTerminate)
    }

    "transition to 'waitingToTerminate' if NoWorkLeft'" in withWorkingWorker() { (worker, _, _, work, _) ⇒
      worker ! NoWorkLeft
      assertWorkerStatus(worker, Worker.WaitingToTerminate)
    }

    "transition to 'WaitingToTerminate' if Retire'" in withWorkingWorker() { (worker, _, routeeProbe, work, _) ⇒
      worker ! Worker.Retire
      assertWorkerStatus(worker, Worker.WaitingToTerminate)
    }

    "terminate after work is done" in withWorkingWorker() { (worker, _, routeeProbe, work, _) ⇒
      watch(worker)
      worker ! Worker.Retire

      expectNoMsg(50.milliseconds)

      routeeProbe.reply(Result("done"))
      expectMsg("done")
      expectTerminated(worker)

    }

    "should only send responses for the current executing Work" in {
      import kanaloa.TestUtils.HandlerProviders._
      //create a new worker whose Routee is actually a Router which simply sends messages to a specific Actor
      //This is so that we can control the response order of who gets messages
      val queueProbe = TestProbe("queue")
      val routeeA = TestProbe("routeeA")
      val routeeB = TestProbe("routeeB")
      val routerActor = system.actorOf(Props(classOf[SimpleRoutingActor], Set(routeeA.ref, routeeB.ref)))
      val handler = simpleHandler(routerActor)
      val metricsCollectorProbe = TestProbe("metricsCollector")
      val worker = TestActorRef[Worker[Any]](Worker.default(queueProbe.ref, handler, metricsCollectorProbe.ref))

      //send the first message, with an aggressive timeout, just so we can have this message timeout
      queueProbe.send(worker, Work(RoutedMessage(routeeA.ref, "AMessage"), Some(self), WorkSettings(timeout = 1.millisecond)))

      routeeA.expectMsg("AMessage")
      expectMsgType[WorkTimedOut]

      queueProbe.expectMsgType[RequestWork]
      //now send a message to the second actor
      queueProbe.send(worker, Work(RoutedMessage(routeeB.ref, "BMessage"), Some(self), WorkSettings(timeout = 10.minutes)))

      routeeB.expectMsg("BMessage")
      //Now, have A respond, it should be ignored
      routeeA.reply(Result("A Result!"))

      routeeB.reply(Result("B Result!"))

      expectMsg("B Result!")
      expectNoMsg(30.milliseconds)
    }

  }

}

class SimpleRoutingActor(actors: Set[ActorRef]) extends Actor {
  override def receive: Receive = {
    case RoutedMessage(routee, message) ⇒
      actors.find(_ == routee).foreach { _.forward(message) }
  }
}

case class RoutedMessage(routee: ActorRef, message: String)
