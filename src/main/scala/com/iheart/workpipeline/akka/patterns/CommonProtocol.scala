package com.iheart.workpipeline.akka.patterns

import akka.actor.ActorRef

import scala.concurrent.duration._

object CommonProtocol {
  case class QueryStatus(replyTo: Option[ActorRef] = None)(implicit sender: ActorRef) {
    def reply(msg: Any)(implicit replier: ActorRef): Unit = {
      replyTo.getOrElse(sender).!(msg)(replier)
    }
  }

  case class ShutdownGracefully(reportBackTo: Option[ActorRef] = None, timeout: FiniteDuration = 3.minutes)
  case object ShutdownSuccessfully

}
