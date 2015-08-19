package com.iheart.poweramp.common.akka.patterns

import akka.actor.ActorRef

object CommonProtocol {
  case class QueryStatus(replyTo: Option[ActorRef] = None)(implicit sender: ActorRef) {
    def reply(msg: Any)(implicit replier: ActorRef): Unit = {
      replyTo.getOrElse(sender).!(msg)(replier)
    }
  }

  case class WorkRejected(reason: String)
  case class WorkFailed(reason: String)
  case class WorkTimedOut(reason: String)
}
