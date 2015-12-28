package kanaloa.util

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable}

import scala.concurrent.duration.FiniteDuration

trait MessageScheduler {

  this: Actor with ActorLogging ⇒

  def delayedMsg(delay: FiniteDuration, msg: Any, receiver: ActorRef = self): Cancellable = {
    import context.dispatcher
    context.system.scheduler.scheduleOnce(delay, receiver, msg)
  }
  def maybeDelayedMsg(delayO: Option[FiniteDuration], msg: Any, receiver: ActorRef = self): Option[Cancellable] = {
    delayO.map(delayedMsg(_, msg, receiver)).orElse {
      receiver ! msg
      None
    }
  }

}
