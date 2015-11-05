package kanaloa.reactive.dispatcher

import akka.actor.{ Actor, Props, ActorRef }
import scala.reflect._

import scala.concurrent.Future

object Backend {

  def apply(actorRef: ActorRef): Backend = (_) ⇒ actorRef
  def apply(props: Props): Backend = f ⇒ f.actorOf(props, "backend")
  def apply[ReqT: ClassTag, ResponseT](f: ReqT ⇒ Future[ResponseT]): Backend = apply(Props(new SimpleDelegatee[ReqT, ResponseT](f)))

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
