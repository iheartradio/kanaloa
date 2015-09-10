package com.iheart.workpipeline.akka.patterns

import akka.actor.ActorRef

object CommonProtocol {
  case class QueryStatus(replyTo: Option[ActorRef] = None)(implicit sender: ActorRef) {
    def reply(msg: Any)(implicit replier: ActorRef): Unit = {
      replyTo.getOrElse(sender).!(msg)(replier)
    }
  }
}
