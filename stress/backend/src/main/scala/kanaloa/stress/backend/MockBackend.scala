package kanaloa.stress.backend

import akka.actor.Props
import akka.contrib.throttle.TimerBasedThrottler
import akka.contrib.throttle.Throttler._
import akka.actor._
import com.typesafe.config.{ Config, ConfigFactory }
import scala.concurrent.duration._
import scala.util.Random

object MockBackend {

  def props(maxThroughput: Option[Int], cfg: Config = ConfigFactory.load("backend.conf")) = {

    val optimalConcurrency = cfg.getInt("optimal-concurrency")
    maxThroughput.foreach { op =>
      assert(op / 10d / optimalConcurrency >= 1, s"throughput $op too low to manager should be at least ${10 * optimalConcurrency}")

    }
    Props(new BackendRouter(
      optimalConcurrency,
      maxThroughput.getOrElse(cfg.getInt("optimal-throughput")),
      cfg.getInt("buffer-size"),
      Some(cfg.getDouble("overload-punish-factor"))
    ))
  }

  /**
   *
   * @param optimalConcurrency
   * @param optimalThroughput maximum number of requests can handle per second
   */
  class BackendRouter(
      optimalConcurrency: Int,
      optimalThroughput: Int,
      bufferSize: Int = 10000,
      overloadPunishmentFactor: Option[Double] = None
  ) extends Actor with ActorLogging {

    var requestsHandling = 0

    val rand = new Random(System.currentTimeMillis())
    val perResponderRate = Math.round(optimalThroughput.toDouble / 10d / optimalConcurrency).toInt msgsPer 100.milliseconds
    val baseLatency = perResponderRate.duration / perResponderRate.numberOfCalls

    val responders: Array[ActorRef] = {

      log.info(s"Per responder rate is set as $perResponderRate")
      Array.tabulate(optimalConcurrency) { _ ⇒
        val throttler = context.actorOf(Props(classOf[TimerBasedThrottler], perResponderRate))
        val responder = context.actorOf(Props(classOf[ResponderBehindThrottler]))
        throttler ! SetTarget(Some(responder))
        throttler
      }
    }

    val receive: Receive = {
      case Request("overflow") ⇒
        log.warning("Overflow command received. Switching to unresponsive mode.")
        context become bufferOverflow
      case Request(msg) ⇒
        requestsHandling += 1

        if (requestsHandling > bufferSize) {
          log.error("!!!! Buffer overflow at, declare dead" + bufferSize)
          context become bufferOverflow
        }

        // the overload punishment is caped at 0.5 (50% of the throughput)
        val overloadPunishment: Double = if (requestsHandling > optimalConcurrency) Math.min(
          0.5,
          overloadPunishmentFactor.fold(0d)(_ * (requestsHandling.toDouble - optimalConcurrency.toDouble) / requestsHandling.toDouble)
        )
        else 0

        val index: Int = if (responders.length > 1)
          Math.round(rand.nextDouble() * (1 - overloadPunishment) * (responders.length - 1)).toInt
        else 0

        val latencyPunishment = overloadPunishment * 3

        if (rand.nextDouble() > 0.995)
          log.debug(s"Extra throughput punishment is $overloadPunishment with $requestsHandling concurrent requests")

        responders(index) ! Petition(msg, sender, (baseLatency * (1d + latencyPunishment)).asInstanceOf[FiniteDuration])

      case Appease(msg, replyTo) ⇒
        requestsHandling -= 1
        replyTo ! Respond(msg)

      case other ⇒ log.error("unknown msg received " + other)
    }

    val bufferOverflow: Receive = {
      case Request("back") ⇒
        log.info("Back command received. Switching back to normal mode.")
        context become receive
      case _ => //just pretend to be dead
    }
  }

  class ResponderBehindThrottler extends Actor with ActorLogging {
    import context.dispatcher
    def receive = {
      case Petition(msg, replyTo, latency) ⇒
        context.system.scheduler.scheduleOnce(latency, sender(), Appease(msg, replyTo))
      case other ⇒ log.error("unknown msg received " + other)
    }
  }

  case class Request(msg: String)
  case class Respond(msg: String)
  case class Petition(msg: String, replyTo: ActorRef, latency: FiniteDuration)
  case class Appease(msg: String, replyTo: ActorRef)
}

