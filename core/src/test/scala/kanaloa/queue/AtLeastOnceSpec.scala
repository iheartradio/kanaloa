package kanaloa.queue

import akka.actor.{PoisonPill, ActorSystem}
import akka.testkit.TestProbe
import kanaloa.ApiProtocol.WorkTimedOut
import kanaloa.ApiProtocol.WorkTimedOut
import kanaloa.SpecWithActorSystem
import kanaloa.TestUtils.Factories
import kanaloa.TestUtils.HandlerProviders
import kanaloa.TestUtils._
import kanaloa.queue.AtLeastOnceSpec.ALOScope
import kanaloa.queue.CircuitBreakerSpec.{CBScope, RequestMsg}
import kanaloa.SpecWithActorSystem
import kanaloa.queue.Queue.Enqueue
import kanaloa.queue.WorkerPoolManager.{WorkerFactory}
import org.scalatest.concurrent.Eventually
import concurrent.duration._

class AtLeastOnceSpec extends SpecWithActorSystem with Eventually {

  "At-least-once delivery" should {

    "keep trying to deliver when there is no request timeout set" in new ALOScope(requestTimeout = None) {
      queue ! Enqueue(RequestMsg, sendResultsTo = Some(self))
      serviceProbe.expectMsg(RequestMsg)
      serviceProbe.expectMsg(RequestMsg)
      serviceProbe.expectMsg(RequestMsg)
      serviceProbe.expectMsg(RequestMsg)
      expectNoMsg(100.milliseconds)

      pool ! PoisonPill
    }

    "keep trying to deliver until the work expires according to request timeout set" in new ALOScope(serviceTimeout = 100.milliseconds, requestTimeout = Some(290.milliseconds)) {

      queue ! Enqueue(RequestMsg, sendResultsTo = Some(self))
      serviceProbe.expectMsg(RequestMsg)
      serviceProbe.expectMsg(RequestMsg)
      serviceProbe.expectMsg(RequestMsg)

      //at this point at least 300ms has passed
      expectMsgType[WorkTimedOut]
      serviceProbe.expectNoMsg(100.milliseconds)

      pool ! PoisonPill

    }
  }

}

object AtLeastOnceSpec {

  abstract class ALOScope(serviceTimeout: FiniteDuration = 20.milliseconds, requestTimeout: Option[FiniteDuration] = None)(implicit system: ActorSystem, factories: Factories) {
    lazy val serviceProbe = TestProbe()
    lazy val handler = HandlerProviders.simpleHandler(serviceProbe)
    lazy val queue = system.actorOf(
      Queue.default(
        TestProbe().ref,
        WorkSettings(serviceTimeout = serviceTimeout, requestTimeout = requestTimeout, atLeastOnce = true)
      )
    )

    val pool = system.actorOf(factories.workerPoolManagerProps(
      queue,
      handler,
      settings = factories.fixedPoolSetting(4)
    ))

  }

  case object RequestMsg
}
