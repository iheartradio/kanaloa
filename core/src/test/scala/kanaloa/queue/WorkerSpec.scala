package kanaloa.queue

import akka.actor.{ActorSystem, ActorRef}
import akka.testkit.{TestActorRef, TestProbe}
import kanaloa.ApiProtocol.QueryStatus
import kanaloa.handler.{Handler, GeneralActorRefHandler}
import kanaloa.queue.Queue.RequestWork
import kanaloa.SpecWithActorSystem
import kanaloa.queue.WorkerPoolSampler.WorkerWorking
import org.scalatest.{Matchers, WordSpecLike, OptionValues}
import org.scalatest.concurrent.Eventually

case class Result(value: Any)
case class Fail(value: String)
import kanaloa.TestUtils.HandlerProviders._

abstract class WorkerSpec extends SpecWithActorSystem with Eventually with OptionValues {

  final def createWorker(circuitBreakerSettings: Option[CircuitBreakerSettings]) = {
    val queueProbe = TestProbe("queue")
    val routeeProbe = TestProbe("routee")
    val metricsCollectorProbe = TestProbe("metricsCollector")
    val worker = TestActorRef[Worker[Any]](
      Worker.default[Any](
        queueProbe.ref,
        simpleHandler(routeeProbe),
        metricsCollectorProbe.ref,
        circuitBreakerSettings
      )
    )
    (queueProbe, routeeProbe, worker, metricsCollectorProbe)
  }

  final def assertWorkerStatus(worker: ActorRef, status: Worker.WorkerStatus) {
    worker ! QueryStatus()
    expectMsg(status)
  }

  final def withIdleWorker(circuitBreakerSettings: Option[CircuitBreakerSettings] = None)(test: (TestActorRef[Worker[Any]], TestProbe, TestProbe, TestProbe) ⇒ Any) {
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
  )(test: (TestActorRef[Worker[Any]], TestProbe, TestProbe, Work[Any], TestProbe) ⇒ Any) {
    withIdleWorker(circuitBreakerSettings) { (worker, queueProbe, routeeProbe, metricCollectorProbe) ⇒
      val work = Work[Any]("work", Some(self), settings)
      queueProbe.send(worker, work) //send it work, to put it into the Working state
      routeeProbe.expectMsg(work.messageToDelegatee) //work should always get sent to a Routee from an Idle Worker
      metricCollectorProbe.expectMsg(WorkerWorking)
      test(worker, queueProbe, routeeProbe, work, metricCollectorProbe)
    }
  }
}

class WorkerFunctionSpec extends WordSpecLike with Matchers {
  import Worker.descriptionOf
  "descriptionOf" should {
    "not truncate if it's below maxLength" in {
      descriptionOf("this is a short message", 100) shouldBe Some("this is a short message")
    }

    "truncate at the first whitespace after maxLength" in {
      descriptionOf("this is a short message", 12) shouldBe Some("this is a short...")
    }

    "truncate at maxLength if there is no whitespace within" in {
      descriptionOf("thisisashortmessage", 4) shouldBe Some("this...")
    }
  }
}
