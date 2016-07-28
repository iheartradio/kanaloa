package com.iheart.kanaloa.stress.http

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import com.iheart.kanaloa.stress.backend.MockBackend
import akka.pattern.{ AskTimeoutException, ask }
import akka.util.Timeout
import kanaloa.reactive.dispatcher.ApiProtocol.WorkFailed
import scala.util.{ Failure, Success }
import scala.io.StdIn._
import com.typesafe.config.ConfigFactory
import kanaloa.reactive.dispatcher.PushingDispatcher
import scala.concurrent.duration._

object StressHttpFrontend extends App {
  implicit val system = ActorSystem("Stress-Tests")
  implicit val materializer = ActorMaterializer()
  implicit val execCtx = system.dispatcher
  implicit val timeout = Timeout(100.seconds)

  val cfg = ConfigFactory.parseResources("stressTestInfra.conf")

  case class Failed(msg: String)

  val backend = system.actorOf(
    Props(new MockBackend.BackendRouter(
      cfg.getInt("sweetSpot"),
      cfg.getInt("minRate")
    )),
    name = "backend"
  )

  val dispatcher =
    system.actorOf(PushingDispatcher.props(
      name = "my-service1",
      backend
    ) {
      case MockBackend.Message(msg) ⇒ Right(msg)
      case other ⇒ Left("Dispatcher: MockBackend.Message() acceptable only. Received: " + other)
    })

  var destination: ActorRef = _
  val destFlag = cfg.getBoolean("kanaloa")
  val route =
    get {
      path(Segment) { msg ⇒
        if (destFlag) { destination = dispatcher }
        else { destination = backend }
        val f = destination ? MockBackend.Message(msg)
        onComplete(f) {
          case Success(WorkFailed(msg)) ⇒ failWith(new Exception(s"work failed: $msg"))
          case Success(msg: String) ⇒ complete("Success! " + msg)
          case Success(other) ⇒ failWith(new Exception(s"unexpected error from backend: $msg"))
          case Failure(e) ⇒ failWith(e)
        }
      } ~
        path("crash") {
          sys.error("Hitting the ../crash url deliberately causes a sys.error...why did you hit it?")
        }
    }

  val bindingFuture = Http().bindAndHandle(route, "localhost", 8081)

}

