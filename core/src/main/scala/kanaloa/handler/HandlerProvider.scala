package kanaloa.handler

import akka.actor.{ActorSystem, ActorRef, ActorRefFactory}
import akka.agent.Agent
import kanaloa.handler.GeneralActorRefHandler._
import kanaloa.handler.HandlerProvider.{HandlersRemoved, HandlersAdded, Subscriber, HandlerChange}
import akka.agent
import kanaloa.util.AnyEq._

import scala.concurrent.{Promise, ExecutionContext, Future}

trait HandlerProvider[-T] {
  def handlers: List[Handler[T]]
  def subscribe(s: Subscriber): Unit
}

object HandlerProvider extends HandlerProviderFactories {
  type Subscriber = HandlerChange ⇒ Unit
  sealed abstract class HandlerChange extends Product with Serializable
  case class HandlersAdded(newHandlers: List[Handler[_]]) extends HandlerChange
  case class HandlersRemoved(removedHandlers: List[Handler[_]]) extends HandlerChange
}

trait HandlerProviderFactories {

  def singleFuture[T](handlerFuture: Future[Handler[T]])(implicit ex: ExecutionContext): HandlerProvider[T] = new SingleFuture(handlerFuture)
  def single[T](handler: Handler[T])(implicit ex: ExecutionContext): HandlerProvider[T] = new SingleFuture(Future.successful(handler))

  private class SingleFuture[T](handlerFuture: Future[Handler[T]])(implicit val ex: ExecutionContext) extends AgentHandlerProvider[T] {
    handlerFuture foreach addHandler
  }

  def actorRef[TResp, TError](
    name:    String,
    futureA: Future[ActorRef],
    factory: ActorRefFactory
  )(resultChecker: ResultChecker[TResp, TError])(implicit ex: ExecutionContext): HandlerProvider[Any] =
    new SingleActorHandlerProvider(futureA, factory)(ar ⇒
      new GeneralActorRefHandler[TResp, TError](name, ar, factory)(resultChecker))

  def actorRef[TResp, TError](
    name:    String,
    ar:      ActorRef,
    factory: ActorRefFactory
  )(resultChecker: ResultChecker[TResp, TError])(implicit ex: ExecutionContext): HandlerProvider[Any] =
    actorRef(name, Future.successful(ar), factory)(resultChecker)

  def actorRef[TResp, TError](
    name: String,
    ar:   ActorRef
  )(resultChecker: ResultChecker[TResp, TError])(implicit system: ActorSystem): HandlerProvider[Any] = {
    import system.dispatcher
    actorRef(name, ar, system)(resultChecker)
  }

}

trait AgentHandlerProvider[T] extends HandlerProvider[T] {
  implicit def ex: ExecutionContext
  val agentSubscribers = Agent[List[Subscriber]](Nil)
  val agentHandlers = Agent[List[Handler[T]]](Nil)

  def handlers: List[Handler[T]] = agentHandlers.get

  def subscribe(s: Subscriber): Unit =
    agentSubscribers.send(s :: _)

  protected def broadcast(change: HandlerChange): Unit =
    agentSubscribers.get.foreach(_(change))

  protected def addHandler(handler: Handler[T]): Future[Boolean] = {
    val resultP = Promise[Boolean]()
    agentHandlers.alter { current ⇒
      if (!current.map(_.name).contains(handler.name)) {
        resultP.trySuccess(true)
        handler :: current
      } else {
        resultP.trySuccess(false)
        current
      }
    }
    resultP.future.map { result ⇒
      if (result)
        broadcast(HandlersAdded(List(handler)))
      result
    }

  }

  protected def removeHandler(handler: Handler[T]): Unit = {
    val exists = agentHandlers.get.exists(_ === handler)
    val result = agentHandlers.alter(_.filterNot(_ === handler))
    if (exists)
      result.foreach(_ ⇒ broadcast(HandlersRemoved(List(handler))))
  }

}
