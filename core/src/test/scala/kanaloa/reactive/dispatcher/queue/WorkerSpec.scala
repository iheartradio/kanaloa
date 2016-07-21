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

object SimpleResultChecker extends ResultChecker {

  override def apply(v1: Any): Either[String, Any] = {
    v1 match {
      case Result(v) ⇒ Right(v): Either[String, Any]
      case Fail(v)   ⇒ Left(v): Either[String, Any]
    }
  }

  override def isDefinedAt(x: Any): Boolean = true
}

abstract class WorkerSpec extends SpecWithActorSystem with Eventually with OptionValues {

  final def createWorker(circuitBreakerSettings: Option[CircuitBreakerSettings]) = {
    val queueProbe = TestProbe("queue")
    val routeeProbe = TestProbe("routee")
    val metricsCollectorProbe = TestProbe("metricsCollector")
    val worker = TestActorRef[Worker](
      Worker.default(
        queueProbe.ref,
        routeeProbe.ref,
        metricsCollectorProbe.ref,
        circuitBreakerSettings
      )(SimpleResultChecker)
    )
    (queueProbe, routeeProbe, worker, metricsCollectorProbe)
  }

  final def assertWorkerStatus(worker: ActorRef, status: Worker.WorkerStatus) {
    worker ! QueryStatus()
    expectMsg(status)
  }

  final def withIdleWorker(circuitBreakerSettings: Option[CircuitBreakerSettings] = None)(test: (TestActorRef[Worker], TestProbe, TestProbe, TestProbe) ⇒ Any) {
    val (queueProbe, routeeProbe, worker, metricsCollectorProbe) = createWorker(circuitBreakerSettings)
    watch(worker)
    try {
      queueProbe.expectMsg(RequestWork(worker)) //should ALWAYS HAPPEN when a Worker starts up
      test(worker, queueProbe, routeeProbe, metricsCollectorProbe)
    } finally {
      unwatch(worker)
      worker.stop()
    }
  }

  final def withWorkingWorker(
    settings:               WorkSettings                   = WorkSettings(),
    circuitBreakerSettings: Option[CircuitBreakerSettings] = None
  )(test: (TestActorRef[Worker], TestProbe, TestProbe, Work, TestProbe) ⇒ Any) {
    withIdleWorker(circuitBreakerSettings) { (worker, queueProbe, routeeProbe, metricCollectorProbe) ⇒
      val work = Work("work", Some(self), settings)
      queueProbe.send(worker, work) //send it work, to put it into the Working state
      routeeProbe.expectMsg(work.messageToDelegatee) //work should always get sent to a Routee from an Idle Worker
      test(worker, queueProbe, routeeProbe, work, metricCollectorProbe)
    }
  }
}
