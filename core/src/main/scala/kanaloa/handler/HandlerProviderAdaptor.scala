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

  implicit def id[T]: HandlerProviderAdaptor[HandlerProvider[T], T] = identity(_)

  implicit def fromSimpleFunction[TReq, TResp](implicit ex: ExecutionContext): HandlerProviderAdaptor[TReq ⇒ Future[TResp], TReq] =
    (f: TReq ⇒ Future[TResp]) ⇒ HandlerProvider.single(new SimpleFunctionHandler(f, s"AnonymousFunction-$index.incrementAndGet()")) //todo: is there a better way to auto name
}
