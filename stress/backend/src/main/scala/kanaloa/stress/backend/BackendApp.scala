package kanaloa.stress.backend

import akka.actor.ActorSystem
import akka.cluster.Cluster
import com.typesafe.config.ConfigFactory

import scala.concurrent.{ Await, Promise }
import scala.concurrent.duration._

object BackendApp extends App {
  val throughput = args.headOption.map(_.toInt)
  val props = MockBackend.props(maxThroughput = throughput)
  val system = ActorSystem("kanaloa-stress", ConfigFactory.load("backend.conf"))
  system.actorOf(props, "backend")

  sys.addShutdownHook {
    println("JVM shutting down, leaving cluster first")
    val cluster = Cluster(system)
    cluster.leave(cluster.selfAddress)
    val promise = Promise[Unit]()
    cluster.registerOnMemberRemoved {
      promise.trySuccess(())
    }

    Await.ready(promise.future, 3.seconds)
    println("Left the cluster")

  }
}
