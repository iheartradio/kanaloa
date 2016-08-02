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
    rampUsers(850) over (3 minutes) //mainly by throttle below
  )).throttle(
    reachRps(100) in (1.minutes),
    holdFor(1.minutes),
    reachRps(400) in (3.minutes),
    holdFor(1.minute)
  )
    .protocols(httpConf)
    .assertions(global.responseTime.percentile3.lessThan(5000)) //95% less than 5s
}
