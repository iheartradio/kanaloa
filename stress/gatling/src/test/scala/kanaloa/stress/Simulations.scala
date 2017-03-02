package kanaloa.stress
import io.gatling.core.Predef._
import io.gatling.core.structure.{ PopulationBuilder, ChainBuilder }
import io.gatling.http.Predef._

import scala.concurrent.duration._

import scala.language.postfixOps

import Infrastructure._

class KanaloaLocalUnderUtilizedSimulation extends Simulation {

  setUp(
    Users(
      numOfUsers = 12,
      path = "kanaloa",
      throttle = Some(80)
    )
  ).protocols(http.disableCaching)
    .assertions(
      global.requestsPerSec.gte(50),
      global.successfulRequests.percent.gte(90)
    )
}

class KanaloaLocalOverflowSimulation extends Simulation {

  setUp(
    Users(
      numOfUsers = 500,
      path = "kanaloa",
      throttle = Some(300), //the capacity is 200 Rps
      rampUp = 30.seconds,
      duration = 3.minutes
    )
  ).protocols(http.disableCaching)
    .assertions(
      global.requestsPerSec.gte(150),
      global.responseTime.percentile3.lte(2500),
      global.successfulRequests.percent.gte(60)
    )
}

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

class KanaloaLoadBalanceOverflowSimulation extends Simulation {
  setUp(
    Users(
      numOfUsers = 800,
      path = "cluster_kanaloa",
      throttle = Some(800),
      rampUp = 10.seconds,
      duration = 45.seconds
    )
  ).protocols(http.disableCaching)
    .assertions(
      global.requestsPerSec.gte(400),
      global.responseTime.percentile3.lte(3000),
      global.successfulRequests.percent.gte(50)
    )
}

class KanaloaOverheadGaugeSimulation extends Simulation {
  setUp(
    Users(
      numOfUsers = 20,
      path = "kanaloa_unthrottled",
      throttle = Some(500000),
      rampUp = 1.seconds
    )
  ).protocols(http.disableCaching)
    .assertions(
      global.requestsPerSec.gte(800),
      global.responseTime.percentile3.lte(4),
      global.successfulRequests.percent.gte(100)
    )
}

class BaselineOverheadGaugeSimulation extends Simulation {
  setUp(
    Users(
      numOfUsers = 20,
      path = "straight_unthrottled",
      throttle = Some(500000),
      rampUp = 1.seconds
    )
  ).protocols(http.disableCaching)
    .assertions(
      global.requestsPerSec.gte(800),
      global.responseTime.percentile3.lte(4),
      global.successfulRequests.percent.gte(100)
    )
}

/**
 * Kanaloa LB most basic without stress.
 */
class KanaloaLoadBalanceNoStressSimulation extends Simulation {
  setUp(
    Users(
      numOfUsers = 200,
      path = "cluster_kanaloa",
      throttle = Some(400),
      rampUp = 1.seconds
    )
  ).protocols(http.disableCaching)
    .assertions(
      global.requestsPerSec.gte(100),
      global.responseTime.percentile3.lte(5000),
      global.successfulRequests.percent.gte(90)
    )
}

class KanaloaLoadBalanceOneNodeLeavingSimulation extends Simulation {
  setUp(
    Users(
      numOfUsers = 1000,
      path = "cluster_kanaloa",
      throttle = Some(200),
      rampUp = 1.seconds
    ),
    CommandSchedule(Command("stop", Some("3000")), services(1), 30.seconds),
    CommandSchedule(Command("start"), services(1), 55.seconds)
  ).protocols(http.disableCaching)
    .assertions(
      global.requestsPerSec.gte(140),
      global.responseTime.percentile3.lte(3000),
      global.successfulRequests.percent.gte(90),
      global.failedRequests.count.lte(2)
    )
}

class KanaloaLoadBalanceOneNodeUnresponsiveSimulation extends Simulation {
  setUp(
    Users(
      numOfUsers = 1000,
      path = "cluster_kanaloa",
      throttle = Some(200),
      rampUp = 1.seconds
    ),
    CommandSchedule(Command("unresponsive"), services(1), 30.seconds),
    CommandSchedule(Command("back-online"), services(1), 55.seconds)
  ).protocols(http.disableCaching)
    .assertions(
      global.requestsPerSec.gte(140),
      global.responseTime.percentile3.lte(3000),
      global.successfulRequests.percent.gte(90),
      global.failedRequests.count.lte(100)
    )
}

class KanaloaLoadBalanceOneNodeSlowThroughputSimulation extends Simulation {
  setUp(
    Users(
      numOfUsers = 200,
      path = "cluster_kanaloa",
      throttle = Some(230),
      rampUp = 1.seconds,
      duration = 3.minutes
    ),
    CommandSchedule(Command("scale", Some("0.25")), services(1), 1.seconds),
    CommandSchedule(Command("restart"), services(1), 200.seconds)
  ).protocols(http.disableCaching)
    .assertions(
      global.requestsPerSec.gte(80),
      global.responseTime.percentile3.lte(1000),
      global.successfulRequests.percent.gte(99),
      global.failedRequests.count.lte(1)
    )
}

class BaselineLoadBalanceOneNodeUnresponsiveSimulation extends Simulation {
  setUp(
    Users(
      numOfUsers = 1000,
      path = "round_robin",
      throttle = Some(200),
      rampUp = 1.seconds
    ),
    CommandSchedule(Command("unresponsive"), services(1), 30.seconds),
    CommandSchedule(Command("back-online"), services(1), 55.seconds)
  ).protocols(http.disableCaching)
    .assertions(
      global.requestsPerSec.gte(140),
      global.responseTime.percentile3.lte(3000),
      global.successfulRequests.percent.gte(90),
      global.failedRequests.count.lte(100)
    )
}

class KanaloaLoadBalanceOneNodeJoiningSimulation extends Simulation {
  setUp(
    Users(
      numOfUsers = 1000,
      path = "cluster_kanaloa",
      throttle = Some(400),
      rampUp = 1.seconds
    ),
    CommandSchedule(Command("stop", Some("3000")), services(1), 10.milliseconds),
    CommandSchedule(Command("start"), services(1), 30.seconds)
  ).protocols(http.disableCaching)
    .assertions(
      global.responseTime.percentile2.lte(2000),
      global.successfulRequests.percent.gte(65)
    )
}
