package kanaloa.stress.backend

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.{ Await, Promise }
import scala.concurrent.duration._

object Main {

  def main(args: Array[String]): Unit = {
    val port = args.headOption.map(_.toInt).getOrElse(8888)

    implicit val system = ActorSystem("kanaloa-stress", ConfigFactory.load("backend.conf"))

    val service = new Service

    service.start(None)

    val server = new CommandServer(port, service)

    sys.addShutdownHook {
      println("JVM shutting down, leaving cluster first")
      val cluster = Cluster(system)
      cluster.leave(cluster.selfAddress)
      val promise = Promise[Unit]()
      cluster.registerOnMemberRemoved {
        promise.trySuccess(())
      }

      Await.ready(promise.future, 3.seconds)
      server.close()
      println("Left the cluster. Closed server")
    }
  }
}

