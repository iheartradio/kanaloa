package kanaloa.stress
import io.gatling.core.Predef._

import io.gatling.core.structure.PopulationBuilder
import io.gatling.http.Predef._
import io.gatling.core.structure.PopulationBuilder
import scala.language.implicitConversions
import scala.concurrent.duration._

object Infrastructure {

  case class ServiceInstance(port: Int, host: String = "localhost") {
    val hostUrl = s"http://$host:$port/"
  }

  case class Command(name: String, args: Option[String] = None)

  trait GatlingRunnable {
    def run: PopulationBuilder
  }

  case class CommandSchedule(cmd: Command, service: ServiceInstance, at: FiniteDuration) extends GatlingRunnable {

    lazy val url = s"${service.hostUrl}command/${cmd.name}" + cmd.args.fold("")(a => s"/$a")

    val run: PopulationBuilder =
      scenario(cmd.name + at).exec(
        pause(at)
      ).exec(
          http(cmd.name)
            .get(url)
            .check(status.is(200))
        ).inject(atOnceUsers(1))
  }

  case class Users(
      path: String,
      numOfUsers: Int = 300,
      name: String = "Users",
      rampUp: FiniteDuration = 1.seconds,
      throttle: Option[Int] = Some(200),
      host: String = "localhost",
      port: Int = 8081,
      duration: FiniteDuration = 1.minute
  ) extends GatlingRunnable {

    val url = s"http://$host:$port/$path/test-1"

    val run: PopulationBuilder = {
      val base = scenario(name).during(duration) {
        exec(
          http(name)
            .get(url)
            .check(status.is(200))
        )
      }.inject(rampUsers(numOfUsers) over (rampUp))
      throttle.fold(base) { t ⇒
        base.throttle(
          reachRps(t) in rampUp,
          holdFor(300.minutes) //has to set a duration here, in reality this is caped by the total duration
        )
      }
    }

  }

  implicit def toPB(gatlingRunnable: GatlingRunnable): PopulationBuilder = gatlingRunnable.run

  val services = List(
    ServiceInstance(8888),
    ServiceInstance(8889)
  )
}
