package kanaloa.research

import io.gatling.core.Predef.global
import io.gatling.core.scenario.Simulation
import io.gatling.http.Predef.http
import kanaloa.stress.Infrastructure._

import scala.language.postfixOps

import scala.concurrent.duration._

abstract class LocalOverflowSimulation(path: String) extends Simulation {
  setUp(
    Users(
      numOfUsers = 900,
      path = path,
      throttle = Some(250), //the capacity is 150-200 Rps
      rampUp = 3.minutes,
      duration = 20.minutes
    )
  ).protocols(http.disableCaching)
    .assertions(
      global.requestsPerSec.gte(150),
      global.responseTime.percentile3.lte(2500),
      global.successfulRequests.percent.gte(60)
    )
}

class KanaloaLocalOverflowSimulation extends LocalOverflowSimulation("kanaloa")

class BaselineLocalOverflowSimulation extends LocalOverflowSimulation("straight")

abstract class NegativeOverflowSimulation(path: String, duration: FiniteDuration = 15.minutes) extends Simulation {

  setUp(
    Users(
      numOfUsers = 900,
      path = path,
      throttle = Some(180), //the capacity is 150-200 Rps
      rampUp = 1.seconds,
      duration = duration
    ),
    CommandSchedule(Command("scale", Some("0.7")), services(0), (duration * 0.1).asInstanceOf[FiniteDuration]),
    CommandSchedule(Command("scale", Some("1.5")), services(0), (duration * 0.6).asInstanceOf[FiniteDuration]),
    CommandSchedule(Command("restart"), services(0), (duration + 10.seconds).asInstanceOf[FiniteDuration])
  ).protocols(http.disableCaching)
    .assertions(
      global.requestsPerSec.gte(150),
      global.responseTime.percentile3.lte(2500),
      global.successfulRequests.percent.gte(60)
    )
}

class KanaloaNegativeOverflowSimulation extends NegativeOverflowSimulation("cluster_kanaloa")

class BaselineNegativeOverflowSimulation extends NegativeOverflowSimulation("round_robin")

/**
 * Baseline LB without kanaloa
 */
class BaselineRoundRobinOverflowSimulation extends Simulation {
  setUp(
    Users(
      numOfUsers = 800,
      path = "round_robin",
      throttle = Some(800),
      rampUp = 10.seconds,
      duration = 45.seconds
    )
  ).protocols(http.disableCaching)
}
