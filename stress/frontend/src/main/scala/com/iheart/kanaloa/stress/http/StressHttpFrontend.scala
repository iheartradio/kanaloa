package com.iheart.kanaloa.stress.http

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import com.iheart.kanaloa.stress.backend.MockBackend
import akka.pattern.{ AskTimeoutException, ask }
import akka.util.Timeout
import kanaloa.reactive.dispatcher.ApiProtocol.{ WorkRejected, WorkTimedOut, WorkFailed }
import scala.util.{ Failure, Success }
import scala.io.StdIn._
import com.typesafe.config.ConfigFactory
import kanaloa.reactive.dispatcher.PushingDispatcher
import scala.concurrent.duration._

object StressHttpFrontend extends App {
  val cfg = ConfigFactory.load("stressTestInfra.conf")

  implicit val system = ActorSystem("Stress-Tests", cfg.resolve())
  implicit val materializer = ActorMaterializer()
  implicit val execCtx = system.dispatcher
  implicit val timeout = Timeout(100.seconds)

  case class Failed(msg: String)

  val backend = system.actorOf(
    Props(new MockBackend.BackendRouter(
      cfg.getInt("optimal-concurrency"),
      cfg.getInt("optimal-throughput")
    )),
    name = "backend"
  )

  val dispatcher =
    system.actorOf(PushingDispatcher.props(
      name = "my-service1",
      backend,
      cfg
    ) {
      case r: MockBackend.Respond ⇒
        Right(r)
      case other ⇒
        Left("Dispatcher: MockBackend.Respond() acceptable only. Received: " + other)
    })

  var destination: ActorRef = _
  val destFlag = cfg.getBoolean("use-kanaloa")
  val route =
    get {
      path(Segment) { msg ⇒
        if (destFlag) { destination = dispatcher }
        else { destination = backend }
        val f = destination ? MockBackend.Request(msg)
        onComplete(f) {
          case Success(WorkRejected(msg)) ⇒ failWith(new Exception(s"Rejected: $msg"))
          case Success(WorkFailed(msg)) ⇒ failWith(new Exception(s"Failed: $msg"))
          case Success(WorkTimedOut(msg)) ⇒ failWith(new Exception(s"Timeout: $msg"))
          case Success(MockBackend.Respond(msg)) ⇒ complete("Success! " + msg)
          case Success(unknown) ⇒ failWith(new Exception(s"unknown response: $unknown"))
          case Failure(e) ⇒ failWith(e)
        }
      } ~
        path("crash") {
          sys.error("Hitting the ../crash url deliberately causes a sys.error...why did you hit it?")
        }
    }

  val bindingFuture = Http().bindAndHandle(route, "localhost", 8081)

  println(s"Server online at http://localhost:8081/\nPress RETURN to stop...")

  readLine()

  bindingFuture.flatMap(_.unbind()).onComplete { _ ⇒ system.terminate() }
}

