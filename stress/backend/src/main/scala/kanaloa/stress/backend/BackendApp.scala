package kanaloa.stress.backend

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

object BackendApp extends App {
  val throughput = args.headOption.map(_.toInt)
  val props = MockBackend.props(maxThroughput = throughput)
  val system = ActorSystem("kanaloa-stress", ConfigFactory.load("backend.conf"))
  system.actorOf(props, "backend")
}
