package kanaloa

import akka.actor._
import akka.testkit.TestProbe
import kanaloa.MockServices.SuicidalActor
import kanaloa.IntegrationTests.Success
import kanaloa.handler.{AgentHandlerProvider, GeneralActorRefHandler, Handler, HandlerProvider}
import kanaloa.handler.HandlerProvider.HandlerChange
import kanaloa.queue.{Fail, Result}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}

object TestHandlerProviders {
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

  def nameFromActorRef(actorRef: ActorRef): String =
    actorRef.path.toString.replace('/', '-')

  def simpleHandler(testProbe: TestProbe)(implicit system: ActorSystem): Handler[Any] =
    simpleHandler(testProbe.ref)

  def simpleHandler(testActor: ActorRef)(implicit system: ActorSystem): Handler[Any] =
    simpleHandler(testActor, simpleResultChecker)

  def simpleHandler[TErr, TResp](testActor: ActorRef, resultChecker: Any ⇒ Either[Option[TErr], TResp])(implicit system: ActorSystem): Handler[Any] =
    new GeneralActorRefHandler(nameFromActorRef(testActor), testActor, system)(resultChecker)

  def simpleHandlerProvider(testProbe: TestProbe)(implicit system: ActorSystem): HandlerProvider[Any] = {
    import system.dispatcher
    HandlerProvider.single(new GeneralActorRefHandler(nameFromActorRef(testProbe.ref), testProbe.ref, system)(simpleResultChecker))
  }

  class MockHandlerProvider(implicit val sys: ActorSystem) extends AgentHandlerProvider[Any] {
    override implicit def ex: ExecutionContext = sys.dispatcher

    def createHandler(testProbe: TestProbe = TestProbe()): Handler[Any] = {
      val newHandler: Handler[Any] = simpleHandler(testProbe)
      addHandler(newHandler)
      newHandler
    }

    def injectHandler(newHandler: Handler[Any]): Unit = {
      addHandler(newHandler)
    }

    def terminateHandler(handler: Handler[Any]): Unit = {
      removeHandler(handler)
    }

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
