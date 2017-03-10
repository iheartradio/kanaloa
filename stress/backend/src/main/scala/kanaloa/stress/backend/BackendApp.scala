package kanaloa.stress.backend

import scala.concurrent.Await
import scala.concurrent.duration._

object Main {

  def main(args: Array[String]): Unit = {
    val port = args.headOption.map(_.toInt).getOrElse(8888)

    val server = new CommandServer(port)

    sys.addShutdownHook {
      println("JVM shutting down, leaving cluster first")
      Await.ready(server.close(), 3.seconds)
      println("Left the cluster. Closed server")
    }
  }
}

