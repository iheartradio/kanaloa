import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.concurrent.duration._
import scala.language.postfixOps

class KanaloaSimulation extends Simulation {
  val Url = "http://localhost:8081/kanaloa-test-1"

  val httpConf = http
    .disableCaching

  val scn = scenario("stress-test").forever {
    group("kanaloa") {
      exec(
        http("flood")
          .get(Url)
          .check(status.is(200))
      )
    }
  }

  setUp(scn.inject(
    rampUsers(100) over (3.minutes)
  )).throttle(
    jumpToRps(2000),
    holdFor(5.minutes)
  )
    .protocols(httpConf)
    .maxDuration(5.minutes)
    .assertions(global.failedRequests.percent.lessThan(50))
}
