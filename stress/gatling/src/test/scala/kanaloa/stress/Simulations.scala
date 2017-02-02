package kanaloa.stress
import io.gatling.core.Predef._
import io.gatling.core.structure.{ PopulationBuilder, ChainBuilder }
import io.gatling.http.Predef._

import scala.concurrent.duration._

import scala.language.postfixOps

import Infrastructure._

class BasicUnderUtilizedSimulation extends Simulation {

  setUp(
    Users(
      numOfUsers = 12,
      path = "kanaloa",
      throttle = Some(80)
    )
  ).protocols(http.disableCaching)
    .maxDuration(1.minute)
    .assertions(
      global.requestsPerSec.gte(50),
      global.successfulRequests.percent.gte(90)
    )
}

class BasicOverflownSimulation extends Simulation {

  setUp(
    Users(
      path = "kanaloa",
      throttle = Some(120),
      rampUp = 1.minute
    )
  ).protocols(http.disableCaching)
    .maxDuration(5.minute)
    .assertions(
      global.requestsPerSec.gte(80),
      global.responseTime.percentile3.lte(5000),
      global.successfulRequests.percent.gte(40)
    )
}

class BasicRoundRobinSimulation extends Simulation {
  setUp(
    Users(
      numOfUsers = 30,
      path = "round_robin",
      throttle = Some(180),
      rampUp = 1.seconds
    )
  ).protocols(http.disableCaching)
    .maxDuration(1.minute)
    .assertions(
      global.requestsPerSec.gte(100),
      global.responseTime.percentile3.lte(5000),
      global.successfulRequests.percent.gte(40)
    )
}

/**
 * Baseline without kanaloa
 */
class BasicBaselineSimulation extends Simulation {
  setUp(
    Users(
      numOfUsers = 18,
      path = "straight",
      throttle = Some(300)
    )
  ).protocols(http.disableCaching)
    .maxDuration(1.minute)
    .assertions(
      global.requestsPerSec.gte(50),
      global.successfulRequests.percent.gte(90)
    )
}

