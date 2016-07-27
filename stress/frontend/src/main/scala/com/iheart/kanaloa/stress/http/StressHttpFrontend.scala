/*
* Copyright [2016] [iHeartMedia Inc]
* All rights reserved
*/
package com.iheart.kanaloa.stress.http

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import com.iheart.kanaloa.stress.backend.MockBackend
import akka.pattern.{AskTimeoutException, ask}
import akka.util.Timeout
import scala.util.{Failure, Success}
import scala.io.StdIn._
import java.io.File
import com.typesafe.config.ConfigFactory
import kanaloa.reactive.dispatcher.PushingDispatcher
import scala.concurrent.duration._

object StressHttpFrontend extends App {
  implicit val system = ActorSystem("Stress-Tests")
  implicit val materializer = ActorMaterializer()
  implicit val execCtx = system.dispatcher
  implicit val timeout = Timeout(100.seconds)

  val cfg = ConfigFactory.parseFile(
    new File("./stress/frontend/config/stressTestInfra.conf")
  )

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
      case _                        ⇒ Left("something fishy going on...")
    })

  var destination: ActorRef = _
  val destFlag = cfg.getBoolean("kanaloa")
  val route =
    get {
      path(Segment) { msg ⇒
        if (destFlag) { destination = dispatcher }
        else { destination = backend }
        val f = destination ? MockBackend.Message(msg) recover {
          case a: AskTimeoutException ⇒ throw new Exception("backend did not respond: \n" + a.getMessage)
        }
        onComplete(f) {
          case Success(value) ⇒ complete("Success! " + value.toString)
          case Failure(e)     ⇒ complete("Timeout from the backend: \n" + e.getMessage)
          case _              ⇒ complete("Other failure type")
        }
      } ~
        path("crash") {
          sys.error("BOOM!")
        }
    }

  val bindingFuture = Http().bindAndHandle(route, "localhost", 8081)

  println(s"Server online at http://localhost:8081/\nPress RETURN to stop...")
  readLine()

  bindingFuture.flatMap(_.unbind()).onComplete { _ ⇒ system.terminate() }
}

