package kanaloa

import akka.actor._
import akka.testkit.TestProbe
import kanaloa.MockServices.SuicidalActor
import kanaloa.IntegrationTests.Success
import kanaloa.handler.{GeneralActorRefHandler, Handler, HandlerProvider}
import kanaloa.handler.HandlerProvider.HandlerChange
import kanaloa.queue.{Fail, Result}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}

object HandlerProviders {
  def fromPromise[TError, TResp](
    promise:       Promise[ActorRef],
    resultChecker: Any ⇒ Either[Option[TError], TResp]
  )(
    implicit
    ex: ExecutionContext, system: ActorSystem
  ): HandlerProvider[Any] = HandlerProvider.actorRef("testPromise", promise.future, system)(resultChecker)

  val simpleResultChecker: Any ⇒ Either[Option[String], Any] = (v1: Any) ⇒ {
    v1 match {
      case Result(v) ⇒ Right(v)
      case Fail(v)   ⇒ Left(Option(v))
    }
  }

  def simpleHandler(testProbe: TestProbe)(implicit system: ActorSystem): Handler[Any] =
    simpleHandler(testProbe.ref)

  def simpleHandler(testActor: ActorRef)(implicit system: ActorSystem): Handler[Any] =
    new GeneralActorRefHandler("routeeHandler", testActor, system)(simpleResultChecker)

  def simpleHandlerProvider(testProbe: TestProbe)(implicit system: ActorSystem): HandlerProvider[Any] = {
    import system.dispatcher
    HandlerProvider.single(new GeneralActorRefHandler("routeeHandler", testProbe.ref, system)(simpleResultChecker))
  }

}

trait MockServices {

  def processTimeBackend(processTime: FiniteDuration): Props = MockServices.delayedProcessorProps(processTime)

  def suicidal(delay: FiniteDuration): Props = Props(classOf[SuicidalActor], delay)
}

object MockServices {
  class SuicidalActor(delay: FiniteDuration) extends Actor {
    import context.dispatcher
    context.system.scheduler.scheduleOnce(delay, self, PoisonPill)
    def receive: Receive = PartialFunction.empty
  }

  class DelayedProcessor(processTime: FiniteDuration) extends Actor {
    import context.dispatcher
    override def receive: Receive = {
      case m ⇒
        context.system.scheduler.scheduleOnce(processTime, sender, Success)
    }
  }

  def delayedProcessorProps(processTime: FiniteDuration): Props = Props(new DelayedProcessor(processTime))
}
