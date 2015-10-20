package com.iheart.workpipeline.akka.patterns.queue

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit._
import com.iheart.workpipeline.akka.{SpecWithActorSystem, patterns}
import com.iheart.workpipeline.metrics.{Metric, MetricsCollector, NoOpMetricsCollector}
import org.specs2.specification.Scope
import org.specs2.mock.Mockito
import scala.concurrent.duration._

import TestUtils._

class WorkerMetricsSpec extends SpecWithActorSystem with Mockito {
  "send WorkCompleted, WorkFailed, and WorkTimedOut metrics" in new WorkerScope {
    val mc = mock[MetricsCollector]
    val workerProps: Props = Worker.default(
      TestProbe().ref,
      Props.empty,
      mc)(resultChecker)
    val worker: ActorRef = TestActorRef(workerProps)

    worker ! Work("some work")
    worker ! MessageProcessed("I did it")

    worker ! Work("really hard work")
    worker ! MessageFailed

    worker ! Work("work with an impossible time limit", WorkSettings(timeout = 0.seconds))

    there was after(100.milliseconds).
      one(mc).send(Metric.WorkCompleted) andThen
      one(mc).send(Metric.WorkFailed) andThen
      one(mc).send(Metric.WorkTimedOut)
  }
}

class WorkerScope(implicit system: ActorSystem)
  extends TestKit(system) with ImplicitSender with Scope


