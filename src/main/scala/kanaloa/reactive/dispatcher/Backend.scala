package kanaloa.reactive.dispatcher

import akka.actor.{ActorRefFactory, Actor, Props, ActorRef}
import scala.reflect._
import scala.concurrent.Future
import scala.language.implicitConversions

trait Backend {
  def apply(f: ActorRefFactory): ActorRef
}

object Backend {

  def apply(f: ActorRefFactory ⇒ ActorRef): Backend = new Backend {
    def apply(factory: ActorRefFactory): ActorRef = f(factory)
  }

  trait BackendAdaptor[T] {
    def apply(t: T): Backend
  }

  object BackendAdaptor extends BackendAdaptors

  trait BackendAdaptors {

    // helper to create an adapator from a function
    def apply[T](f: T ⇒ Backend) = new BackendAdaptor[T] {
      def apply(t: T): Backend = f(t)
    }

    implicit val backendBackend = apply[Backend](identity)

    // accepting subtypes of ActorRef to also support TestActorRef
    implicit def actorRefBackend[T <: ActorRef] = apply[T](ref ⇒ Backend(_ ⇒ ref))

    implicit val propsBackend = apply[Props](props ⇒ Backend(_.actorOf(props)))

    implicit def delegateeeBackend[ReqT: ClassTag, ResponseT] = apply[ReqT ⇒ Future[ResponseT]](
      f ⇒ Backend(_.actorOf(Props(new SimpleDelegatee[ReqT, ResponseT](f))))
    )

    case class UnexpectedRequest(request: Any) extends Exception

    private class SimpleDelegatee[ReqT: ClassTag, ResponseT](f: ReqT ⇒ Future[ResponseT]) extends Actor {
      import context.dispatcher
      def receive: Receive = {
        case t: ReqT if classTag[ReqT].runtimeClass.isInstance(t) ⇒
          val replyTo = sender
          f(t).recover {
            case e: Throwable ⇒ e
          }.foreach(replyTo ! _)

        case m ⇒ sender ! UnexpectedRequest(m)
      }
    }

  }

  implicit def backendAdapaterToBackend[T](t: T)(implicit adaptor: BackendAdaptor[T]): Backend = adaptor(t)

}

