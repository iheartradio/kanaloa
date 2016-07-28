package com.iheart.kanaloa.stress.backend

import akka.actor.Props
import akka.contrib.throttle.TimerBasedThrottler
import akka.contrib.throttle.Throttler._
import akka.actor._
import scala.concurrent.duration._
import scala.util.Random

object MockBackend {

  class BackendRouter(maxRoutees: Int, setRate: Int) extends Actor with ActorLogging {
    import context.dispatcher
    var activeRoutees: List[ActorRef] = _
    val rand = new Random(System.currentTimeMillis())
    val throttlers = List.tabulate(maxRoutees)(n ⇒ context.actorOf(Props(classOf[TimerBasedThrottler], setRate msgsPer 1.second)))

    override def preStart() = {
      throttlers.foreach(_ ! SetTarget(Some(context.actorOf(Props(classOf[BackendResponder])))))
      activeRoutees = throttlers.take(1)
    }

    def reThrottle() = {
      if (EntryActor.counter < maxRoutees && EntryActor.counter > 0) {
        activeRoutees = throttlers.take(EntryActor.counter)
        log.debug("concurrent work: " + EntryActor.counter + ", new rate: " + EntryActor.counter)
      } else if (EntryActor.counter >= maxRoutees) {
        val rate = -math.pow(EntryActor.counter - maxRoutees, 2) / (0.5 * maxRoutees) + maxRoutees
        val newRate = rate.toInt
        if (newRate > 0) {
          activeRoutees = throttlers.take(newRate)
          log.debug("concurrent work: " + EntryActor.counter + ", new rate: " + newRate)
        } else {
          activeRoutees = throttlers.take(1)
          log.debug("too much work, new rate: " + newRate)
        }
      }
    }

    def receive = {
      case Message(msg) ⇒
        EntryActor.counter += 1
        reThrottle()
        log.debug("message() received in Router")
        val random_index = rand.nextInt(activeRoutees.length)
        val responder = activeRoutees(random_index)
        responder ! Petition(msg, sender, this.self)

      case Appease(msg, replyTo) ⇒
        EntryActor.counter -= 1
        log.debug("appease() received in Router")
        replyTo ! Message(msg)

      case _ ⇒ sender ! Message("don't recognize!")
    }
  }

  class BackendResponder extends Actor with ActorLogging {
    import context.dispatcher
    def receive = {
      case Petition(msg, roundTrip, source) ⇒
        context.system.scheduler.scheduleOnce(50.milliseconds) {
          log.debug("petition() received in Responder")
          source ! Appease(msg, roundTrip)
        }
      case _ ⇒ log.error("received unknown message in Responder")
    }
  }

  object EntryActor {
    var counter = 0
  }
  case class Message(msg: String)
  case class Petition(msg: String, roundTrip: ActorRef, source: ActorRef)
  case class Appease(msg: String, replyTo: ActorRef)
}

