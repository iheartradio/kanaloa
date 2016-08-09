package kanaloa.stress.http

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.routing.FromConfig
import akka.stream.ActorMaterializer
import akka.pattern.{ AskTimeoutException, ask }
import akka.util.Timeout
import kanaloa.reactive.dispatcher.ApiProtocol.{ WorkRejected, WorkTimedOut, WorkFailed }
import kanaloa.stress.backend.MockBackend
import kanaloa.util.JavaDurationConverters._
import scala.util.{ Failure, Success }
import scala.io.StdIn._
import com.typesafe.config.ConfigFactory
import kanaloa.reactive.dispatcher.PushingDispatcher
import scala.concurrent.duration._

object StressHttpFrontend extends App {
  val cfg = ConfigFactory.load("frontend.conf")

  implicit val system = ActorSystem("kanaloa-stress", cfg.resolve())
  implicit val materializer = ActorMaterializer()
  implicit val execCtx = system.dispatcher
  implicit val timeout: Timeout = (cfg.getDuration("frontend-timeout").asScala).asInstanceOf[FiniteDuration]

  case class Failed(msg: String)

  val localBackend = system.actorOf(MockBackend.props, name = "local-backend")
  val remoteBackendRouter = system.actorOf(FromConfig.props(), "backendRouter")

  lazy val dispatcher =
    system.actorOf(PushingDispatcher.props(
      name = "with-local-backend",
      localBackend,
      cfg
    ) {
      case r: MockBackend.Respond ⇒
        Right(r)
      case other ⇒
        Left("Dispatcher: MockBackend.Respond() acceptable only. Received: " + other)
    })

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

  val route = testRoute("kanaloa", dispatcher) ~
    testRoute("straight", localBackend) ~
    testRoute("round_robin", remoteBackendRouter)

  val bindingFuture = Http().bindAndHandle(route, "localhost", 8081)

  println(s"Server online at http://localhost:8081/\nPress RETURN to stop...")

  readLine()

  bindingFuture.flatMap(_.unbind()).onComplete { _ ⇒ system.terminate() }
}

