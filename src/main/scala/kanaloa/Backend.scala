package kanaloa.reactive.dispatcher

import akka.actor.{ ActorRefFactory, Actor, Props, ActorRef }
import kanaloa.reactive.dispatcher.Backend.BackendAdaptor
import scala.reflect._

import scala.concurrent.Future

trait Backend {
  def apply(f: ActorRefFactory): ActorRef
}

object Backend extends BackendAdaptors {

  type BackendAdaptor[T] = T ⇒ Backend

  def apply(f: ActorRefFactory ⇒ ActorRef): Backend = new Backend {
    def apply(factory: ActorRefFactory): ActorRef = f(factory)
  }
}

trait BackendAdaptors {
  implicit val artb: BackendAdaptor[ActorRef] = ar ⇒ Backend((_: ActorRefFactory) ⇒ ar)
  implicit val aptb: BackendAdaptor[Props] = props ⇒ Backend((f: ActorRefFactory) ⇒ f.actorOf(props))
  implicit def aftb[ReqT: ClassTag, ResponseT]: BackendAdaptor[ReqT ⇒ Future[ResponseT]] =
    (f: ReqT ⇒ Future[ResponseT]) ⇒ Backend((factory: ActorRefFactory) ⇒ factory.actorOf(Props(new SimpleDelegatee[ReqT, ResponseT](f))))

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
