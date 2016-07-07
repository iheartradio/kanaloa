package kanaloa.reactive.dispatcher.queue

import akka.actor.{ActorRef, ActorRefFactory, ActorSystem, PoisonPill}
import akka.testkit.{TestActorRef, TestProbe}
import kanaloa.reactive.dispatcher.ApiProtocol.WorkFailed
import kanaloa.reactive.dispatcher.queue.Queue.RequestWork
import kanaloa.reactive.dispatcher.queue.Worker.Hold
import kanaloa.reactive.dispatcher.{Backend, ResultChecker, SpecWithActorSystem}
import org.scalatest.concurrent.Eventually

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
    p = Promise[ActorRef]
    p.future
  }

  def fillActor(actorRef: ActorRef) {
    if (p.isCompleted) {
      throw new IllegalStateException("attempting to complete an already completed Backend..was the constructor called?")
    }
    p.success(actorRef)
  }
}

class WorkerSpec extends SpecWithActorSystem with Eventually {
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

  "worker waitingForRoutee" should {
    "accept Work if Work is not already queued" in new ControlledBackendScope {
      val backendProbe = TestProbe()
      backend.fillActor(backendProbe.ref)
      queueProb.expectMsgType[RequestWork]
      queueProb.reply(Work("w"))
      backendProbe.expectMsg("w")
      worker.stop()
    }

    "reject Work if Work is already queued" in new ControlledBackendScope {
      queueProb.send(worker, Work("work"))
      queueProb.send(worker, Work("work2"))
      queueProb.expectMsgType[Rejected]
      worker.stop()
    }

    "shutdown when told to retire if there is no queued Work" in new ControlledBackendScope {
      watch(worker)
      worker ! Worker.Retire
      expectTerminated(worker)
    }

    "reject queued Work when told to retire" in new ControlledBackendScope {
      worker ! Work("w")
      watch(worker)

      worker ! Worker.Retire

      expectMsg(Rejected(Work("w"), "retiring"))
      expectTerminated(worker)
    }
  }

  "worker waiting" should {
    "recover from dead routee" in new ControlledBackendScope {
      val firstBackendRef = TestProbe("first")
      val newBackEndRef = TestProbe("new")

      backend.fillActor(firstBackendRef.ref)
      queueProb.expectMsgType[RequestWork]

      firstBackendRef.ref ! PoisonPill

      //a little gross, but short of redoing bits of Worker to make this more deterministic, this is the
      //easiest way i can think of ensuring that the Backend constructor was called
      //ideally we would be resetting the Worker.Routee to null..OR changing it to an Option and setting it to None
      eventually {
        backend.p.isCompleted shouldBe false
      }

      //new ref returned to Actor
      backend.fillActor(newBackEndRef.ref)

      eventually {
        worker.underlyingActor.routee shouldBe newBackEndRef.ref
      }

      queueProb.reply(Work("w", replyTo = Some(self)))

      newBackEndRef.expectMsg("w")
      newBackEndRef.reply(Result(1))
      expectMsg(Result(1))

      worker.stop()
    }

    "apply a Hold duration when sending Work to a Routee" in new ControlledBackendScope {
      val backendProbe = TestProbe()
      queueProb.send(worker, Work("work"))
      queueProb.send(worker, Hold(10.milliseconds))

      eventually {
        worker.underlyingActor.delayBeforeNextWork shouldBe Some(10.milliseconds)
      }

      backend.fillActor(backendProbe.ref)
      //i was having trouble using 'expectNoMessage(minimumDuration)', so seeing the value get reset will have to suffice for now
      backendProbe.expectMsg("work")
      worker.underlyingActor.delayBeforeNextWork shouldBe None
      worker.stop()
    }
  }

  "worker working" should {
    "recover from dead routee" in new ControlledBackendScope {
      val firstBackendRef = TestProbe("first")
      val newBackEndRef = TestProbe("new")
      backend.fillActor(firstBackendRef.ref)
      queueProb.expectMsgType[RequestWork]
      queueProb.reply(Work("w", replyTo = Some(self)))

      firstBackendRef.expectMsg("w")
      firstBackendRef.ref ! PoisonPill
      //once the Worker detects the death, the work should be terminated
      expectMsgType[WorkFailed]

      //a little gross, but short of redoing bits of Worker to make this more deterministic, this is the
      //easiest way i can think of ensuring that the Backend constructor was called
      //ideally we would be resetting the Worker.Routee to null..OR changing it to an Option and setting it to None
      eventually {
        backend.p.isCompleted shouldBe false
      }

      //new ref returned to Actor
      backend.fillActor(newBackEndRef.ref)

      eventually {
        worker.underlyingActor.routee shouldBe newBackEndRef.ref
      }

      newBackEndRef.expectNoMsg() //should not get any new work yet
      worker.stop()
    }

    "apply a Hold duration to a subsequent askForWork when Work is finished" in new ControlledBackendScope {
      val backendProbe = TestProbe()
      backend.fillActor(backendProbe.ref)
      queueProb.send(worker, Work("work"))
      backendProbe.expectMsg("work")
      //while the backend is "working", lets send a hold
      queueProb.send(worker, Hold(10.milliseconds))
      eventually {
        worker.underlyingActor.delayBeforeNextWork shouldBe Some(10.milliseconds)
      }
      //let's now finish the work.
      backendProbe.reply(Result(1))
      //i was having trouble using 'expectNoMessage(minimumDuration)', so seeing the value get reset will have to suffice for now
      queueProb.expectMsgType[RequestWork]
      worker.underlyingActor.delayBeforeNextWork shouldBe None
      worker.stop()
    }
  }

  "worker retiring" should {

  }
}
