package kanaloa.reactive.dispatcher.queue

import akka.actor.{ActorRef, ActorRefFactory, ActorSystem, PoisonPill}
import akka.testkit.{TestActorRef, TestProbe}
import kanaloa.reactive.dispatcher.queue.Queue.RequestWork
import kanaloa.reactive.dispatcher.{Backend, ResultChecker, SpecWithActorSystem}

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._

case class Result(value: Any)

class TestBackend(delay: FiniteDuration = Duration.Zero)(implicit system: ActorSystem) extends Backend {
  var prob = TestProbe("backend")
  override def apply(f: ActorRefFactory): Future[ActorRef] = {
    import system.dispatcher
    Future {
      if (delay > Duration.Zero) Thread.sleep(delay.toMillis)
      prob.ref
    }
  }
}

class ControlledBackend()(implicit system: ActorSystem) extends Backend {
  var p = Promise[ActorRef]
  override def apply(f: ActorRefFactory): Future[ActorRef] = {
    p.future
  }

  def fillActor(actorRef: ActorRef) {
    p.success(actorRef)
  }
}

class WorkerSpec extends SpecWithActorSystem {
  def newWorker(queueRef: ActorRef, b: Backend): TestActorRef[Worker] = TestActorRef[Worker](Worker.default(queueRef, b)(ResultChecker.simple[Result]))

  trait WorkerScope {
    val queueProb = TestProbe("queue")
    val backend = new TestBackend()
    val worker = newWorker(queueProb.ref, backend)
  }
  trait ControlledBackendScope {
    val queueProb = TestProbe("queue")
    val backend = new ControlledBackend()
    val worker = newWorker(queueProb.ref, backend)
  }

  "worker starting" should {
    "accept work" in new ControlledBackendScope {
      val backendProbe = TestProbe()
      backend.fillActor(backendProbe.ref)
      queueProb.expectMsgType[RequestWork]
      queueProb.reply(Work("w"))
      backendProbe.expectMsg("w")
    }

    "reject Work if Work is already accepted" in new ControlledBackendScope {
      queueProb.send(worker, Work("work"))
      queueProb.send(worker, Work("work2"))
      queueProb.expectMsgType[Rejected]
    }

    "shutdown when retiring" in new ControlledBackendScope {
      watch(worker)
      worker ! Worker.Retire
      expectTerminated(worker)
    }

    "reject work when retiring" in new ControlledBackendScope {
      worker ! Work("w")
      watch(worker)

      worker ! Worker.Retire

      expectMsgType[Rejected]

      expectTerminated(worker)
    }
  }

  "worker waiting" should {
    "recover from dead routee" in new WorkerScope {
      queueProb.expectMsgType[RequestWork]

      val oldBackendRef = backend.prob.ref

      val newBackendProb = TestProbe("newBackend")
      backend.prob = newBackendProb

      oldBackendRef ! PoisonPill

      awaitAssert(worker.underlyingActor.getRoutee should ===(newBackendProb.ref))
      queueProb.reply(Work("w", replyTo = Some(self)))

      newBackendProb.expectMsg("w")
      newBackendProb.reply(Result(1))
      expectMsg(Result(1))

      worker.stop()
    }
  }

  "worker working" should {
    "recover from dead routee" in new WorkerScope {
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

  "worker retiring" should {

  }

  "worker" should {

    "retrieve work from queue and send to backend" in new WorkerScope {
      queueProb.expectMsgType[RequestWork]
      queueProb.reply(Work("work"))
      backend.prob.expectMsg("work")
      worker.stop()
    }
  }
}
