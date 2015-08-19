package com.iheart.poweramp.common.akka.helpers

import akka.actor.{Cancellable, ActorRef, ActorLogging, Actor}

import scala.concurrent.duration.FiniteDuration

trait MessageScheduler {

  this: Actor with ActorLogging =>

  def delayedMsg(delay: FiniteDuration, msg: Any, receiver: ActorRef = self ): Cancellable = {
    import context.dispatcher
    context.system.scheduler.scheduleOnce(delay, receiver, msg)
  }
}
