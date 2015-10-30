package kanaloa.reactive.dispatcher

import akka.actor.ActorSystem
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import kanaloa.reactive.dispatcher.queue.ProcessingWorkerPoolSettings
import kanaloa.reactive.dispatcher.queue.TestUtils.Wrapper
import org.specs2.specification.Scope

class DispatcherSpec extends SpecWithActorSystem {
  "pulling work dispatcher" should {

    "finish a simple list" in new ScopeWithActor {
      val iterator = List(1, 3, 5, 6).iterator
      val pwp = system.actorOf(PullingDispatcher.props(
        "test",
        iterator,
        Dispatcher.defaultDispatcherSettings.copy(workerPool = ProcessingWorkerPoolSettings(1), autoScalingSettings = None),
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
