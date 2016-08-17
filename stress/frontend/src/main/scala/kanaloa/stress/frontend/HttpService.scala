package kanaloa.stress.frontend

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.routing.FromConfig
import akka.stream.ActorMaterializer
import akka.pattern.{ AskTimeoutException, ask }
import akka.util.Timeout
import kanaloa.reactive.dispatcher.ApiProtocol.{ WorkRejected, WorkTimedOut, WorkFailed }
import kanaloa.stress.backend.BackendApp._
import kanaloa.stress.backend.MockBackend
import kanaloa.util.JavaDurationConverters._
import scala.util.{ Failure, Success }
import scala.io.StdIn._
import com.typesafe.config.ConfigFactory
import kanaloa.reactive.dispatcher.{ ResultChecker, ClusterAwareBackend, PushingDispatcher }
import scala.concurrent.duration._

class HttpService(inCluster: Boolean, maxThroughputRPS: Option[Int] = None) {

  val baseCfg = ConfigFactory.load("frontend.conf")
  val cfg = if (inCluster) baseCfg else baseCfg.withoutPath("akka.actor.provider").withoutPath("akka.cluster").withoutPath("akka.remote")

  implicit val system = ActorSystem("kanaloa-stress", cfg.resolve())
  implicit val materializer = ActorMaterializer()
  implicit val execCtx = system.dispatcher
  implicit val timeout: Timeout = (cfg.getDuration("frontend-timeout").asScala).asInstanceOf[FiniteDuration]

  case class Failed(msg: String)

  lazy val localBackend = system.actorOf(MockBackend.props(
    maxThroughput = maxThroughputRPS
  ), name = "local-backend")

  lazy val localUnThrottledBackend = system.actorOf(MockBackend.props(false), name = "local-direct-backend")

  lazy val remoteBackendRouter = system.actorOf(FromConfig.props(), "backendRouter")

  val resultChecker: ResultChecker = {
    case r: MockBackend.Respond ⇒
      Right(r)
    case other ⇒
      Left("Dispatcher: MockBackend.Respond() acceptable only. Received: " + other)
  }

  lazy val dispatcher =
    system.actorOf(PushingDispatcher.props(
      name = "with-local-backend",
      localBackend,
      cfg
    )(resultChecker), "local-dispatcher")

  lazy val localUnThrottledBackendWithKanaloa = system.actorOf(PushingDispatcher.props(
    name = "with-local-unthrottled-backend",
    localUnThrottledBackend,
    ConfigFactory.parseString(
      """
        |kanaloa.default-dispatcher {
        |  workerPool {
        |    startingPoolSize = 1000
        |    maxPoolSize = 3000
        |  }
        |}
      """.stripMargin
    ).withFallback(cfg)
  )(resultChecker), "local-unthrottled-dispatcher")

  lazy val clusterDispatcher =
    system.actorOf(PushingDispatcher.props(
      name = "with-remote-backend",
      ClusterAwareBackend("backend", "backend"),
      cfg
    )(resultChecker), "cluster-dispatcher")

  def testRoute(rootPath: String, destination: ActorRef) =
    get {
      path(rootPath / (".+"r)) { msg ⇒

        val f = (destination ? MockBackend.Request(msg)).recover {
          case e: akka.pattern.AskTimeoutException => WorkTimedOut(s"ask timeout after $timeout")
        }

        onComplete(f) {
          case Success(WorkRejected(msg)) ⇒ complete(503, "service unavailable")
          case Success(WorkFailed(msg)) ⇒ failWith(new Exception(s"Failed: $msg"))
          case Success(WorkTimedOut(msg)) ⇒ complete(408, msg)
          case Success(MockBackend.Respond(msg)) ⇒ complete("Success! " + msg)
          case Success(unknown) ⇒ failWith(new Exception(s"unknown response: $unknown"))
          case Failure(e) ⇒ complete(408, e)
        }
      }
    }

  val localRoutes = testRoute("kanaloa", dispatcher) ~
    testRoute("straight", localBackend) ~
    testRoute("straight_unthrottled", localUnThrottledBackend) ~
    testRoute("kanaloa_unthrottled", localUnThrottledBackendWithKanaloa)

  lazy val clusterEnabledRoutes =
    testRoute("round_robin", remoteBackendRouter) ~
      testRoute("cluster_kanaloa", clusterDispatcher)

  val routes = if (inCluster) clusterEnabledRoutes ~ localRoutes else localRoutes
  val bindingFuture = Http().bindAndHandle(routes, "localhost", 8081)

  def close(): Unit = {
    bindingFuture.flatMap(_.unbind()).onComplete { _ ⇒ system.terminate() }
  }
}

object HttpService extends App {
  val inCluster = args.headOption.map(_.toBoolean).getOrElse(true)
  println("Starting http service " + (if (inCluster) " in cluster" else ""))

  val service = new HttpService(inCluster, None)
  println(s"Server online at http://localhost:8081/\nPress RETURN to stop...")

  readLine()

  service.close()

}

