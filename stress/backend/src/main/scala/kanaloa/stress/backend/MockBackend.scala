package kanaloa.stress.backend

import akka.actor.Props
import akka.contrib.throttle.TimerBasedThrottler
import akka.contrib.throttle.Throttler._
import akka.actor._
import com.typesafe.config.{ Config, ConfigFactory }
import scala.concurrent.duration._
import scala.util.Random

object MockBackend {

  def props(
    throttle: Boolean = true,
    maxThroughput: Option[Int] = None,
    minLatency: Option[FiniteDuration] = None,
    cfg: Config = ConfigFactory.load("backend.conf")
  ) = {

    val optimalConcurrency = cfg.getInt("optimal-concurrency")

    Props(new BackendRouter(
      throttle,
      optimalConcurrency,
      maxThroughput.getOrElse(cfg.getInt("optimal-throughput")),
      cfg.getInt("buffer-size"),
      minLatency,
      Some(cfg.getDouble("overload-punish-factor"))
    ))
  }

  /**
   *
   * @param initialConcurrency
   * @param initialThroughput maximum number of requests can handle per second
   */
  class BackendRouter(
      throttle: Boolean,
      initialConcurrency: Int,
      initialThroughput: Int,
      bufferSize: Int = 10000,
      minLatency: Option[FiniteDuration] = None,
      overloadPunishmentFactor: Option[Double] = None
  ) extends Actor with ActorLogging {

    var requestsHandling = 0

    val rand = new Random(System.currentTimeMillis())

    var optimalConcurrency = initialConcurrency

    var optimalThroughput = initialThroughput

    var (baseLatency, responders) = startResponders()

    def startResponders(): (FiniteDuration, Array[ActorRef]) = {
      val perResponderRate = Math.round(optimalThroughput.toDouble / 10d / optimalConcurrency).toInt msgsPer 100.milliseconds

      val latency = {
        val latencyFromThroughput = perResponderRate.duration / perResponderRate.numberOfCalls
        minLatency.filter(_ > latencyFromThroughput).getOrElse(latencyFromThroughput)
      }

      log.info(s"setting up $optimalConcurrency responders with base latency as $latency and $perResponderRate each. ")

      log.info(s"Per responder rate is set as $perResponderRate")
      (latency, Array.tabulate(optimalConcurrency) { _ ⇒
        val throttler = context.actorOf(Props(classOf[TimerBasedThrottler], perResponderRate))
        val responder = context.actorOf(Props(classOf[ResponderBehindThrottler]))
        throttler ! SetTarget(Some(responder))
        throttler
      })

    }

    def receive = if (throttle) throttled else direct

    def statusRespond =
      Respond(s"Optimal Concurrency: $optimalConcurrency, Max Throughput: ${optimalThroughput}msg/second, latency: ${baseLatency.toMillis}ms")

    val handleCommands: Receive = {
      case Scale(ratio) =>
        optimalConcurrency = (optimalConcurrency * ratio).toInt
        optimalThroughput = (optimalThroughput * ratio).toInt
        responders.foreach(_ ! PoisonPill)

        val (newBaseLatency, newResponders) = startResponders()

        baseLatency = newBaseLatency
        responders = newResponders

        sender ! statusRespond

      case CheckStatus =>
        sender ! statusRespond

      case Unresponsive ⇒
        log.warning("Overflow command received. Switching to unresponsive mode.")
        context become unresponsive
        sender ! Respond(s"Becoming Unresponsive")

      case ErrorOut ⇒
        log.warning("Error command received. Switching to error mode.")
        context become error
        sender ! Respond(s"Becoming Error all the time")

      case BackOnline ⇒
        log.info("Back command received. Switching back to normal mode.")
        context become throttled
        sender ! Respond("back to normal")

    }

    val throttled: Receive = handleCommands orElse {
      case Request(msg) ⇒
        requestsHandling += 1

        if (requestsHandling > bufferSize) {
          log.error("!!!! Buffer overflow at, declare dead" + bufferSize)
          context become unresponsive
        }

        // the overload punishment is caped at 0.5 (50% of the throughput)
        val overloadPunishment: Double = if (requestsHandling > initialConcurrency) {

          val punishment = overloadPunishmentFactor.fold(0d)(_ * (requestsHandling.toDouble - initialConcurrency.toDouble) / requestsHandling.toDouble)

          Math.min(0.5, punishment)
        } else 0

        val index: Int = if (responders.length > 1)
          Math.round(rand.nextDouble() * (1d - overloadPunishment) * (responders.length - 1)).toInt
        else 0

        val latencyPunishment = overloadPunishment * 3

        if (rand.nextDouble() > 0.995)
          log.info(s"Extra throughput punishment is $overloadPunishment with $requestsHandling concurrent requests")

        responders(index) ! Petition(msg, sender, (baseLatency * (1d + latencyPunishment)).asInstanceOf[FiniteDuration])

      case Appease(msg, replyTo) ⇒
        requestsHandling -= 1
        replyTo ! Respond(msg)

      case other ⇒ log.error("unknown msg received " + other)
    }

    val unresponsive: Receive = handleCommands orElse {
      case _ => //just pretend to be dead
    }

    val error: Receive = handleCommands orElse {
      case _ => sender ! Error("in error mode")
    }

    val direct: Receive = handleCommands orElse {
      case Request(m) => sender ! Respond(m)
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
  sealed trait ControlCommand
  case object Unresponsive extends ControlCommand
  case class Scale(ratio: Double) extends ControlCommand
  case object NewThroughput extends ControlCommand
  case object ErrorOut extends ControlCommand
  case object CheckStatus extends ControlCommand
  case object BackOnline extends ControlCommand

  case class Error(msg: String)
  case class Respond(msg: String)
  case class Petition(msg: String, replyTo: ActorRef, latency: FiniteDuration)
  case class Appease(msg: String, replyTo: ActorRef)
}

