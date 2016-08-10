package kanaloa.stress

import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * Simulation against a local actor with kanaloa in front of it.
 */
class KanaloaLocalSimulation extends OverflowSimulation("kanaloa")

/**
 * Simulation against plain backend without kanaloa
 */
class StraightSimulation extends OverflowSimulation("straight")

/**
 * Simulation against a round robin cluster router without kanaloa
 */
class RoundRobinSimulation extends OverflowSimulation("round_robin")

abstract class OverflowSimulation(path: String) extends Simulation {

  val Url = s"http://localhost:8081/$path/test-1"

  val httpConf = http.disableCaching

  val scn = scenario("stress-test").forever {
    group("kanaloa") {
      exec(
        http("flood")
          .get(Url)
          .check(status.is(200))
      )
    }
  }

  val duration = 30.minutes
  val rampUp = 1.minutes

  setUp(scn.inject(
    rampUsers(2000) over (5 minutes) //mainly by throttle below
  )).throttle(
    reachRps(200) in rampUp,
    holdFor(duration)
  )
    .protocols(httpConf)
    .maxDuration(duration + rampUp)
    .assertions(global.responseTime.percentile3.lessThan(5000)) //95% less than 5s

}
