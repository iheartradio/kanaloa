package kanaloa.stress

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import kanaloa.stress.frontend.HttpService

import scala.concurrent.duration._
import scala.language.postfixOps

class KanaloaLocalSimulation extends StressSimulation(SimulationSettings.overflow(
  path = "kanaloa",
  name = "Overflow a local service with kanaloa"
))

class StraightSimulation extends StressSimulation(SimulationSettings(
  duration = 5.minutes,
  rampUp = 1.minutes,
  path = "straight",
  name = "Overflow a local service without kanaloa"
))

class RoundRobinSimulation extends StressSimulation(
  SimulationSettings(
    duration = 5.minutes,
    rampUp = 1.minutes,
    path = "round_robin",
    name = "Overflow a cluster with round robin router without kanaloa"
  )
)

class FastDirectSimulation extends StressSimulation(
  SimulationSettings.forUnthrottledWithoutCPUPressure(
    path = "straight_unthrottled",
    name = "Against a super fast backend without kanaloa"
  )
)

class FastKanaloaSimulation extends StressSimulation(
  SimulationSettings.forUnthrottledWithoutCPUPressure(
    path = "kanaloa_unthrottled",
    name = "Against a super fast backend with kanaloa"
  )
)

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
    .maxDuration(duration + rampUp + 1.minute)
    .assertions(
      global.responseTime.percentile3.lessThan(ninetyFivePctResponseTimeAssertion.toMillis.toInt),
      global.responseTime.percentile2.lessThan(seventyFivePctResponseTimeAssertion.toMillis.toInt),
      global.requestsPerSec.greaterThan((maxBackendThroughputRPS * 0.75).toInt)
    )

}

case class SimulationSettings(
  duration: FiniteDuration,
  rampUp: FiniteDuration,
  path: String,
  name: String,
  capRps: Int = 200,
  users: Int = 1000,
  responseTimeout: FiniteDuration = 15.seconds,
  ninetyFivePctResponseTimeAssertion: FiniteDuration = 4.seconds,
  seventyFivePctResponseTimeAssertion: FiniteDuration = 2.seconds,
  maxBackendThroughputRPS: Int = 200
)

object SimulationSettings {
  def forUnthrottledWithoutCPUPressure(name: String, path: String) = new SimulationSettings(
    duration = 2.minutes,
    rampUp = 10.seconds,
    users = 20,
    capRps = 500000,
    name = name,
    path = path
  )

  def forUnthrottledWithCPUPressure(name: String, path: String) = new SimulationSettings(
    duration = 2.minutes,
    rampUp = 10.seconds,
    users = 500,
    capRps = 500000,
    name = name,
    path = path
  )

  def overflow(name: String, path: String) = new SimulationSettings(
    duration = 5.minutes,
    rampUp = 1.minutes,
    capRps = 300,
    users = 3000,
    maxBackendThroughputRPS = 200,
    name = name,
    path = path
  )

}
