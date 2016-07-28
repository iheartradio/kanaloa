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
          .check(responseTimeInMillis.lessThan(5000))
      )
    }
  }

  setUp(scn.inject(
    rampUsers(30) over (30)
  )).protocols(httpConf)
    .maxDuration(60.second)
    .assertions(global.failedRequests.percent.lessThan(50))
}
