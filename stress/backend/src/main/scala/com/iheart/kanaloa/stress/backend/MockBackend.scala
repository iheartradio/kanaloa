package com.iheart.kanaloa.stress.backend

import akka.actor.Props
import akka.contrib.throttle.TimerBasedThrottler
import akka.contrib.throttle.Throttler._
import akka.actor._
import scala.concurrent.duration._
import scala.util.Random

object MockBackend {

  /**
   *
   * @param optimalConcurrency
   * @param optimalThroughput maximum number of requests can handle per second
   */
  class BackendRouter(optimalConcurrency: Int, optimalThroughput: Int) extends Actor with ActorLogging {

    var concurrentRequests = 0

    val rand = new Random(System.currentTimeMillis())
    val responders: Array[ActorRef] =
      Array.tabulate(optimalConcurrency) { _ ⇒
        val throttler = context.actorOf(Props(classOf[TimerBasedThrottler], (optimalThroughput.toDouble / 10d / optimalConcurrency).toInt msgsPer 100.milliseconds))
        val responder = context.actorOf(Props(classOf[ResponderBehindThrottler]))
        throttler ! SetTarget(Some(responder))
        throttler
      }

    def limitedRespondersWhenOverload =
      (-math.pow(concurrentRequests - optimalConcurrency, 2) /
        (0.5 * optimalConcurrency) + optimalConcurrency).toInt

    def receive = {
      case Request(msg) ⇒
        concurrentRequests += 1

        val activeResponders = if (concurrentRequests < optimalConcurrency)
          concurrentRequests else limitedRespondersWhenOverload

        log.info("concurrent work: " + concurrentRequests + ", responders: " + (activeResponders + 1))

        val index =
          if (activeResponders > 0)
            rand.nextInt(activeResponders)
          else 0

        responders(index) ! Petition(msg, sender)

      case Appease(msg, replyTo) ⇒
        concurrentRequests -= 1
        replyTo ! Respond(msg)

      case other ⇒ log.error("unknown msg received " + other)
    }
  }

  class ResponderBehindThrottler extends Actor with ActorLogging {
    import context.dispatcher
    def receive = {
      case Petition(msg, replyTo) ⇒
        context.system.scheduler.scheduleOnce(50.milliseconds, sender(), Appease(msg, replyTo))
      case other ⇒ log.error("unknown msg received " + other)
    }
  }

  case class Request(msg: String)
  case class Respond(msg: String)
  case class Petition(msg: String, replyTo: ActorRef)
  case class Appease(msg: String, replyTo: ActorRef)
}

