package kanaloa.reactive.dispatcher

import akka.actor.ActorRef

import scala.concurrent.duration._

object ApiProtocol {
  sealed trait Request
  sealed trait Response
  sealed trait WorkException extends Response

  case class QueryStatus(replyTo: Option[ActorRef] = None)(implicit sender: ActorRef) extends Request {
    def reply(msg: Any)(implicit replier: ActorRef): Unit = {
      replyTo.getOrElse(sender).!(msg)(replier)
    }
  }

  case class ShutdownGracefully(reportBackTo: Option[ActorRef] = None, timeout: FiniteDuration = 3.minutes) extends Request
  case object ShutdownSuccessfully extends Response

  case class WorkRejected(reason: String) extends WorkException
  case class WorkFailed(reason: String) extends WorkException
  case class WorkTimedOut(reason: String) extends WorkException

}
