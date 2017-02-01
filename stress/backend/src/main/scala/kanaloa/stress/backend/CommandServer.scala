package kanaloa.stress.backend

import akka.cluster.Cluster
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import akka.actor._
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.routing.FromConfig
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import kanaloa.stress.backend.MockBackend._

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.io.StdIn._
import scala.util.{ Try, Failure, Success }
import scala.collection.JavaConverters._
import concurrent.duration._

class CommandServer(port: Int, service: Service)(implicit system: ActorSystem) {

  val cfg = ConfigFactory.load()

  implicit val materializer = ActorMaterializer()
  implicit val execCtx = system.dispatcher
  implicit val timeout: Timeout = 1.second

  case class Failed(msg: String)

  def command(cmd: String, arg: Option[String]): Future[Unit] = cmd match {
    case "start" =>
      Future {
        val throughput = arg.flatMap(a => Try(a.toInt).toOption)
        service.start(throughput)
      }
    case "stop" => Future(service.stop())
    case "error" => service.control(ErrorOut)
    case "unresponsive" => service.control(Unresponsive)
    case "back-online" => service.control(BackOnline)
  }

  def cmdResult[T](cmd: String, args: Option[String]) = {
    val f = command(cmd, args)
    onComplete(f) {
      case Success(_) ⇒ complete(s"Successfully $cmd! ")
      case Failure(e) ⇒ complete(408, e)
    }
  }

  val routes =
    get {
      path("command" / (".+"r) / (".+"r)) { (cmd, arg) ⇒
        cmdResult(cmd, Some(arg))
      }
    } ~
      get {
        path("command" / (".+"r))(cmdResult(_, None))
      } ~
      get {
        path("request" / (".+"r)) { r ⇒
          val f = service.request(r)
          onComplete(f) {
            case Success(Right(Respond(m))) ⇒ complete(s"Success! $m")
            case Success(Left(e)) => complete(500, s"Failure! $e")
            case Failure(e) ⇒ complete(408, e)
          }
        }
      }

  println(s"Server started at $port")
  val bindingFuture = Http().bindAndHandle(routes, "localhost", port)

  def close(): Unit = {
    bindingFuture.flatMap(_.unbind()).onComplete { _ ⇒ system.terminate() }
  }
}

class Service(implicit system: ActorSystem) {
  var actor: Option[ActorRef] = None
  val cluster = Cluster(system)
  import system.dispatcher

  def control(cmd: ControlCommand): Future[Unit] = {
    actor.fold(Future.failed[Unit](new Exception("Service is stopped"))) { a =>
      Future.successful(a ! cmd)
    }
  }

  def request(req: String): Future[Either[String, Respond]] = {
    import akka.pattern.ask
    implicit val to: Timeout = 2.seconds
    actor.fold[Future[Either[String, Respond]]](Future.successful(Left("Offline"))) { a =>
      val resp = (a ? Request(req)).mapTo[Respond]
      resp.map(Right(_))
    }
  }

  def start(throughput: Option[Int]): Unit = {
    cluster.join(cluster.selfAddress)
    actor = Some(system.actorOf(MockBackend.props(maxThroughput = throughput), "backend"))
  }

  def stop(): Unit = {
    cluster.leave(cluster.selfAddress)
    actor.foreach(_ ! PoisonPill)
  }
}
