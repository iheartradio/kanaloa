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
      global.requestsPerSec.gte(70),
      global.failedRequests.count.lte(0)
    )
}

class KanaloaLocalOverflowSimulation extends Simulation {

  setUp(
    Users(
      numOfUsers = 900,
      path = "kanaloa",
      throttle = Some(300),
      duration = 2.minutes
    )
  ).protocols(http.disableCaching)
    .assertions(
      global.requestsPerSec.gte(250),
      global.responseTime.percentile2.lte(1000),
      global.responseTime.percentile3.lte(5000), //mainly due to the burst mode
      global.successfulRequests.percent.gte(60)
    )
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
      global.requestsPerSec.gte(450),
      global.responseTime.percentile3.lte(3000),
      global.successfulRequests.percent.gte(60)
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
      global.requestsPerSec.gte(300),
      global.responseTime.percentile3.lte(500),
      global.successfulRequests.percent.gte(100)
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
      global.requestsPerSec.gte(160),
      global.responseTime.percentile3.lte(300),
      global.failedRequests.count.lte(2)
    )
}

class KanaloaLoadBalanceOneNodeUnresponsiveSimulation extends Simulation {
  setUp(
    Users(
      numOfUsers = 900,
      path = "cluster_kanaloa",
      throttle = Some(200),
      rampUp = 1.seconds
    ),
    CommandSchedule(Command("unresponsive"), services(1), 30.seconds),
    CommandSchedule(Command("back-online"), services(1), 55.seconds)
  ).protocols(http.disableCaching)
    .assertions(
      global.requestsPerSec.gte(160),
      global.responseTime.percentile3.lte(300),
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
      duration = 2.minutes
    ),
    CommandSchedule(Command("scale", Some("0.25")), services(1), 1.seconds),
    CommandSchedule(Command("restart"), services(1), 130.seconds)
  ).protocols(http.disableCaching)
    .assertions(
      global.requestsPerSec.gte(180),
      global.responseTime.percentile2.lte(200),
      global.responseTime.percentile3.lte(3000),
      global.successfulRequests.percent.gte(100)
    )
}

class BaselineLoadBalanceOneNodeUnresponsiveSimulation extends Simulation {
  setUp(
    Users(
      numOfUsers = 900,
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
      numOfUsers = 900,
      path = "cluster_kanaloa",
      throttle = Some(400),
      rampUp = 1.seconds,
      duration = 90.seconds
    ),
    CommandSchedule(Command("stop", Some("3000")), services(1), 10.milliseconds),
    CommandSchedule(Command("start"), services(1), 10.seconds)
  ).protocols(http.disableCaching)
    .assertions(
      global.requestsPerSec.gte(350),
      global.responseTime.percentile2.lte(250),
      global.successfulRequests.percent.gte(95)
    )
}
