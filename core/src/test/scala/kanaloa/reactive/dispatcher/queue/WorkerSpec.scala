package kanaloa.reactive.dispatcher.queue

import akka.actor.ActorRef
import akka.testkit.{TestActorRef, TestProbe}
import kanaloa.reactive.dispatcher.ApiProtocol.QueryStatus
import kanaloa.reactive.dispatcher.queue.Queue.RequestWork
import kanaloa.reactive.dispatcher.{ResultChecker, SpecWithActorSystem}
import org.scalatest.OptionValues
import org.scalatest.concurrent.Eventually

case class Result(value: Any)
case class Fail(value: String)

abstract class WorkerSpec extends SpecWithActorSystem with Eventually with OptionValues {

  def resultChecker: ResultChecker = {
    case Result(v) ⇒ Right(v)
    case Fail(v)   ⇒ Left(v)
  }

  def createWorker = {
    val queueProbe = TestProbe("queue")
    val routeeProbe = TestProbe("routee")
    val worker = TestActorRef[Worker](Worker.default(queueProbe.ref, routeeProbe.ref)(resultChecker))
    (queueProbe, routeeProbe, worker)
  }

  def assertWorkerStatus(worker: ActorRef, status: Worker.WorkerStatus) {
    worker ! QueryStatus()
    expectMsg(status)
  }

  def withIdleWorker(test: (TestActorRef[Worker], TestProbe, TestProbe) ⇒ Any) {
    val (queueProbe, routeeProbe, worker) = createWorker
    watch(worker)
    try {
      queueProbe.expectMsg(RequestWork(worker)) //should ALWAYS HAPPEN when a Worker starts up
      test(worker, queueProbe, routeeProbe)
    } finally {
      unwatch(worker)
      worker.stop()
    }
  }

  def withWorkingWorker(settings: WorkSettings = WorkSettings())(test: (TestActorRef[Worker], TestProbe, TestProbe, Work) ⇒ Any) {
    withIdleWorker { (worker, queueProbe, routeeProbe) ⇒
      val work = Work("work", Some(self), settings)
      worker ! work //send it work, to put it into the Working state
      routeeProbe.expectMsg(work.messageToDelegatee) //work should always get sent to a Routee from an Idle Worker
      test(worker, queueProbe, routeeProbe, work)
    }
  }
}
