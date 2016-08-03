package kanaloa.stress.backend

import akka.actor.ActorSystem

object BackendApp extends App {
  val system = ActorSystem()
  system.actorOf(MockBackend.props, "backend")
}
