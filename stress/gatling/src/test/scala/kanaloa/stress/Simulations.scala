package kanaloa.stress
import io.gatling.core.Predef._
import io.gatling.core.structure.{ PopulationBuilder, ChainBuilder }
import io.gatling.http.Predef._

import scala.concurrent.duration._

import scala.language.postfixOps

import Infrastructure._

class BasicUnderStressSimulation extends Simulation {

  setUp(
    Users(
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

/**
 * Baseline without kanaloa
 */
class BasicBaselineSimulation extends Simulation {

  setUp(
    Users(
      path = "straight",
      throttle = Some(80)
    )
  ).protocols(http.disableCaching)
    .maxDuration(1.minute)
    .assertions(
      global.requestsPerSec.gte(50),
      global.successfulRequests.percent.gte(90)
    )
}


