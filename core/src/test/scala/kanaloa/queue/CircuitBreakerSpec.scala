package kanaloa.queue

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import kanaloa.ApiProtocol.WorkTimedOut
import kanaloa.TestUtils._
import kanaloa.queue.CircuitBreakerSpec.{CBScope, RequestMsg}
import kanaloa.SpecWithActorSystem
import kanaloa.queue.Queue.Enqueue
import kanaloa.queue.WorkerPoolManager.{WorkerFactory, CircuitBreakerFactory}
import org.scalatest.concurrent.Eventually
import concurrent.duration._

class CircuitBreakerSpec extends SpecWithActorSystem with Eventually {

  "Circuitbreaker" should {

    "not open before timeout hitting the threshold" in new CBScope(factories) {
      queue ! Enqueue(RequestMsg, sendResultsTo = Some(self))
      queue ! Enqueue(RequestMsg, sendResultsTo = Some(self))
      serviceProbe.expectMsg(RequestMsg)
      serviceProbe.expectMsg(RequestMsg)
      expectMsgType[WorkTimedOut]
      expectMsgType[WorkTimedOut]
    }

    //todo: move this test to dispatcher
    "reply after timeout should be ignore" in new CBScope(factories) {
      queue ! Enqueue(RequestMsg, sendResultsTo = Some(self))
      queue ! Enqueue(RequestMsg, sendResultsTo = Some(self))
      serviceProbe.expectMsg(RequestMsg)
      serviceProbe.expectMsg(RequestMsg)
      expectMsgType[WorkTimedOut]
      expectMsgType[WorkTimedOut]

      serviceProbe.reply("tobe ignored")
      expectNoMsg(100.milliseconds)
    }

    "open after timeout hitting the threshold and close after duration" in new CBScope(factories) {
      queue ! Enqueue(RequestMsg, sendResultsTo = Some(self))
      queue ! Enqueue(RequestMsg, sendResultsTo = Some(self))
      queue ! Enqueue(RequestMsg, sendResultsTo = Some(self))
      serviceProbe.expectMsg(RequestMsg)
      serviceProbe.expectMsg(RequestMsg)
      serviceProbe.expectMsg(RequestMsg)
      expectMsgType[WorkTimedOut]
      expectMsgType[WorkTimedOut]
      expectMsgType[WorkTimedOut]

      Thread.sleep(20) //wait for the open to take effect
      queue ! Enqueue(RequestMsg, sendResultsTo = Some(self))
      serviceProbe.expectNoMsg(80.milliseconds) //no message during CB Open

      serviceProbe.expectMsg(RequestMsg) //after CB closes the message delivers
    }

    "resetting timeout count if it get response" in new CBScope(factories) {
      queue ! Enqueue(RequestMsg, sendResultsTo = Some(self))
      queue ! Enqueue(RequestMsg, sendResultsTo = Some(self))
      serviceProbe.expectMsg(RequestMsg)
      serviceProbe.expectMsg(RequestMsg)

      expectMsgType[WorkTimedOut]
      expectMsgType[WorkTimedOut]

      serviceProbe.reply(Result("toBeIgnored"))
      serviceProbe.reply(Result("toBeIgnored"))

      queue ! Enqueue(RequestMsg)
      serviceProbe.expectMsg(10.milliseconds, RequestMsg)
      serviceProbe.reply(Result("reply"))

      queue ! Enqueue(RequestMsg, sendResultsTo = Some(self))
      queue ! Enqueue(RequestMsg, sendResultsTo = Some(self))

      expectMsgType[WorkTimedOut]
      expectMsgType[WorkTimedOut]

      queue ! Enqueue(RequestMsg)
      serviceProbe.expectMsg(10.milliseconds, RequestMsg)
    }
  }

}

object CircuitBreakerSpec {
  abstract class CBScope(factories: Factories)(implicit system: ActorSystem) {
    lazy val serviceProbe = TestProbe()
    lazy val handler = HandlerProviders.simpleHandler(serviceProbe)
    lazy val queue = system.actorOf(
      Queue.default(
        TestProbe().ref,
        WorkSettings(timeout = 20.milliseconds)
      )
    )

    system.actorOf(factories.workerPoolManagerProps(
      queue,
      handler,
      settings = factories.fixedPoolSetting(4),
      circuitBreakerFactory = Some(
        CircuitBreakerFactory(CircuitBreakerSettings(
          openDurationBase = 100.milliseconds,
          timeoutCountThreshold = 2
        ))
      )
    ))

  }

  case object RequestMsg
}
