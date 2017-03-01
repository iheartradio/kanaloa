package kanaloa.handler

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorRefFactory, ActorRef}
import kanaloa.handler.GeneralActorRefHandler.ResultChecker

import scala.concurrent.{ExecutionContext, Future}

trait HandlerProviderAdaptor[A, -T] extends (A ⇒ HandlerProvider[T])

object HandlerProviderAdaptor {
  private val index = new AtomicInteger(0)

  import scala.language.implicitConversions

  implicit def fromFunc[A, T](f: A ⇒ HandlerProvider[T]) = new HandlerProviderAdaptor[A, T] {
    def apply(a: A): HandlerProvider[T] = f(a)
  }

  implicit def fromSingleHandler[TReq](implicit ex: ExecutionContext) = new HandlerProviderAdaptor[Handler[TReq], TReq] {
    def apply(h: Handler[TReq]): HandlerProvider[TReq] = HandlerProvider.single(h)
  }

  implicit def id[T, H](implicit ev: H <:< HandlerProvider[T]): HandlerProviderAdaptor[H, T] = ev

  implicit def fromSimpleFunction[TReq, TResp](implicit ex: ExecutionContext): HandlerProviderAdaptor[TReq ⇒ Future[TResp], TReq] =
    (f: TReq ⇒ Future[TResp]) ⇒ HandlerProvider.single(new SimpleFunctionHandler(f, s"AnonymousFunction-$index.incrementAndGet()"))

  implicit def fromSimpleActorRef(implicit ex: ExecutionContext, actorRefFactory: ActorRefFactory): HandlerProviderAdaptor[ActorRef, Any] = (actorRef: ActorRef) ⇒
    HandlerProvider.single(GeneralActorRefHandler(actorRef.path.toStringWithoutAddress, actorRef, actorRefFactory)(ResultChecker.complacent))
}
