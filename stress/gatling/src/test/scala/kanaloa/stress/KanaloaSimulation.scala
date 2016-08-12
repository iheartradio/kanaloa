package kanaloa.stress

import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * Simulation against a local actor with kanaloa in front of it.
 */
class KanaloaLocalSimulation extends StressSimulation(SimulationSettings(
  duration = 5.minutes,
  rampUp = 1.minutes,
  path = "kanaloa",
  name = "Overflow a local service with kanaloa"
))

/**
 * Simulation against plain backend without kanaloa
 */
class StraightSimulation extends StressSimulation(SimulationSettings(
  duration = 5.minutes,
  rampUp = 1.minutes,
  path = "straight",
  name = "Overflow a local service without kanaloa"
))

/**
 * Simulation against a round robin cluster router without kanaloa
 */
class RoundRobinSimulation extends StressSimulation(
  SimulationSettings(
    duration = 5.minutes,
    rampUp = 1.minutes,
    path = "round_robin",
    name = "Overflow a cluster with round robin router"
  )
)

/**
 * Simulation against a kanaloa enabled cluster backend
 */
class KanaloaClusterSimulation extends StressSimulation(
  SimulationSettings(
    duration = 5.minutes,
    rampUp = 1.minutes,
    path = "cluster_kanaloa",
    name = "Overflow a cluster with kanaloa in front"
  )
)

abstract class StressSimulation(settings: SimulationSettings) extends Simulation {
  import settings._

  val Url = s"http://localhost:8081/$path/test-1"

  val httpConf = http.disableCaching

  val scn = scenario(name).forever {
    group("kanaloa") {
      exec(
        http("flood")
          .get(Url)
          .check(status.is(200))
          .check(responseTimeInMillis lessThan (responseTimeout.toMillis.toInt))

      )
    }
  }

  setUp(scn.inject(
    rampUsers(users) over (rampUp) //mainly by throttle below
  )).throttle(
    reachRps(capRps) in rampUp,
    holdFor(duration)
  )
    .protocols(httpConf)
    .maxDuration(duration + rampUp)
    .assertions(global.responseTime.percentile3.lessThan(5000)) //95% less than 5s

}

case class SimulationSettings(
  duration: FiniteDuration,
  rampUp: FiniteDuration,
  path: String,
  name: String,
  capRps: Int = 200,
  users: Int = 1000,
  responseTimeout: FiniteDuration = 6.seconds
)
