package kanaloa.reactive.dispatcher.queue

import akka.actor.{PoisonPill, ActorSystem, ActorRef, ActorRefFactory}
import akka.testkit.{TestActorRef, TestProbe}
import kanaloa.reactive.dispatcher.queue.Queue.RequestWork
import kanaloa.reactive.dispatcher.{Backend, ResultChecker, SpecWithActorSystem}

import scala.concurrent.Future

case class Result(value: Any)

class TestBackend(implicit system: ActorSystem) extends Backend {
  var prob = TestProbe("backend")
  override def apply(f: ActorRefFactory): Future[ActorRef] =
    Future.successful(prob.ref)

}

class WorkerSpec extends SpecWithActorSystem {

  "worker" should {

    def newWorker(queueRef: ActorRef, b: Backend): TestActorRef[Worker] = TestActorRef[Worker](Worker.default(queueRef, b)(ResultChecker.simple[Result]))

    "retrieve work from queue and send to backend" in {
      val queueProb = TestProbe("queue")
      val backend = new TestBackend
      val worker = newWorker(queueProb.ref, backend)
      queueProb.expectMsgType[RequestWork]
      queueProb.reply(Work("work"))
      backend.prob.expectMsg("work")
      worker.stop()
    }

    "recover from dead routee while waiting" in {
      val queueProb = TestProbe("queue")
      val backend = new TestBackend
      val worker = newWorker(queueProb.ref, backend)
      queueProb.expectMsgType[RequestWork]

      val oldBackendRef = backend.prob.ref

      val newBackendProb = TestProbe("newBackend")
      backend.prob = newBackendProb

      oldBackendRef ! PoisonPill

      awaitAssert(worker.underlyingActor.getRoutee should ===(newBackendProb.ref))

      queueProb.reply(Work("w", WorkSettings(sendResultTo = Some(self))))

      newBackendProb.expectMsg("w")

      newBackendProb.reply(Result(1))

      expectMsg(Result(1))

      worker.stop()
    }

    "recover from dead routee while working" in {

      val queueProb = TestProbe("queue")
      val backend = new TestBackend
      val worker = newWorker(queueProb.ref, backend)

      queueProb.expectMsgType[RequestWork]
      queueProb.reply(Work("w"))

      backend.prob.expectMsg("w")

      val oldBackendRef = backend.prob.ref

      val newBackendProb = TestProbe("newBackend")
      backend.prob = newBackendProb

      oldBackendRef ! PoisonPill

      awaitAssert(worker.underlyingActor.getRoutee should ===(newBackendProb.ref))

      newBackendProb.expectNoMsg()

      worker.stop()
    }
  }

}
