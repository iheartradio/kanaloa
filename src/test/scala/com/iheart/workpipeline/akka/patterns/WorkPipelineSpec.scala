package com.iheart.workpipeline.akka.patterns

import akka.actor.ActorSystem
import akka.testkit.{ TestProbe, ImplicitSender, TestKit }
import com.iheart.workpipeline.akka.patterns.queue.ProcessingWorkerPoolSettings
import com.iheart.workpipeline.akka.patterns.queue.TestUtils.Wrapper
import org.specs2.mutable.Specification

import com.iheart.workpipeline.akka.{ SpecWithActorSystem, patterns }
import org.specs2.specification.Scope

class WorkPipelineSpec extends SpecWithActorSystem {
  "pulling work pipeline" should {

    "finish a simple list" in new ScopeWithActor {
      val iterator = List(1, 3, 5, 6).iterator
      val pwp = system.actorOf(PullingWorkPipeline.props(
        "test",
        iterator,
        WorkPipeline.defaultWorkPipelineSettings.copy(workerPool = ProcessingWorkerPoolSettings(1), autoScalingSettings = None),
        delegateeProps
      )({ case Success ⇒ Right(()) }))

      delegatee.expectMsg(1)
      delegatee.reply(Success)
      delegatee.expectMsg(3)
      delegatee.reply(Success)
      delegatee.expectMsg(5)
      delegatee.reply(Success)
      delegatee.expectMsg(6)
      delegatee.reply(Success)
    }
  }
}

class ScopeWithActor(implicit system: ActorSystem) extends TestKit(system) with ImplicitSender with Scope {
  case object Success

  val delegatee = TestProbe()

  val delegateeProps = Wrapper.props(delegatee.ref)
}
