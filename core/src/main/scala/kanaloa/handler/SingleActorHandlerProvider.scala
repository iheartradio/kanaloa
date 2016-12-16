package kanaloa.handler

import akka.actor._

import scala.concurrent.{Future, ExecutionContext}

class SingleActorHandlerProvider(
  actorF:          Future[ActorRef],
  actorRefFactory: ActorRefFactory
)(
  handlerCreator: ActorRef ⇒ Handler[Any]
)(implicit val ex: ExecutionContext) extends AgentHandlerProvider[Any] {

  private var handler: Option[Handler[Any]] = None

  actorF.foreach { actor ⇒
    handler = Some(handlerCreator(actor))
    addHandler(handler.get)
    actorRefFactory.actorOf(SingleActorHandlerProvider.monitorProps(actor, () ⇒ handler.foreach(removeHandler)))
  }

}

object SingleActorHandlerProvider {
  private class ActorMonitor(actor: ActorRef, onTerminate: () ⇒ Unit) extends Actor with ActorLogging {
    context.watch(actor)

    def receive: Receive = {
      case Terminated(`actor`) ⇒
        onTerminate()
        log.error(s"service actor ${actor.path} is terminated.")
        context stop self
    }
  }
  def monitorProps(actor: ActorRef, onTerminate: () ⇒ Unit): Props = Props(new ActorMonitor(actor, onTerminate))
}
