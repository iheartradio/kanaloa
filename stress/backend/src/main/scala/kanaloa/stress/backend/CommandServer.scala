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

import scala.concurrent.{ Promise, Future }
import scala.concurrent.duration.FiniteDuration
import scala.io.StdIn._
import scala.util.{ Try, Failure, Success }
import scala.collection.JavaConverters._
import concurrent.duration._

class CommandServer(port: Int) {

  @volatile
  var service: Option[Service] = Some(new Service)

  implicit val system: ActorSystem = ActorSystem("command-server")
  val cfg = ConfigFactory.load()

  implicit val materializer = ActorMaterializer()
  implicit val execCtx = system.dispatcher
  implicit val timeout: Timeout = 1.second

  case class Failed(msg: String)

  def startService(throughput: Option[Int]): Future[Respond] = Future {
    if (service.isEmpty) {
      service = Some(new Service(throughput))
      Respond("started")
    } else
      Respond("service already started")
  }

  def stopService(delay: Option[Int]): Future[Respond] =
    service.fold(Future.successful(Respond("Already stopped")))(_.stop(delay).map { _ =>
      service = None
      Respond("stopped")
    })

  def command(cmd: String, arg: Option[String]): Future[Respond] = {
    lazy val intArg: Option[Int] =
      arg.flatMap(a => Try(a.toInt).toOption)

    cmd match {
      case "start" =>
        startService(intArg)

      case "stop" =>
        stopService(intArg)

      case "restart" =>
        for {
          _ <- stopService(None)
          r <- startService(None)
        } yield r

      case "scale" => runCmd(Scale(arg.get.toDouble))
      case "error" => runCmd(ErrorOut)
      case "unresponsive" => runCmd(Unresponsive)
      case "check" => runCmd(CheckStatus)
      case "back-online" => runCmd(BackOnline)
    }
  }

  def runCmd(cmd: ControlCommand): Future[Respond] = {
    service.fold(Future.successful(Respond("service stopped")))(s => s.control(cmd))
  }

  def cmdResult[T](cmd: String, args: Option[String]) = {
    val f = command(cmd, args)
    onComplete(f) {
      case Success(Respond(msg)) ⇒ complete(s"Successfully $cmd! result: $msg ")
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
          val f = service.fold[Future[Either[String, Respond]]](Future.successful(Left("server stopped")))(_.request(r))
          onComplete(f) {
            case Success(Right(Respond(m))) ⇒ complete(s"Success! $m")
            case Success(Left(e)) => complete(500, s"Failure! $e")
            case Failure(e) ⇒ complete(408, e)
          }
        }
      }

  println(s"Server started at $port")
  val bindingFuture = Http().bindAndHandle(routes, "localhost", port)

  def close(): Future[Unit] = {
    for {
      _ <- bindingFuture.flatMap(_.unbind()).map { _ ⇒ system.terminate() }
      _ <- service.fold(Future.successful(()))(_.stop())
    } yield ()
  }
}

class Service(throughput: Option[Int] = None) {

  implicit val system = ActorSystem("kanaloa-stress", ConfigFactory.load("backend.conf"))
  val cluster = Cluster(system)

  val seeds: collection.immutable.Seq[Address] = List(AddressFromURIString(
    system.settings.config.getString("cluster-seed")
  ))

  cluster.joinSeedNodes(seeds)

  val actor = system.actorOf(MockBackend.props(maxThroughput = throughput), "backend")

  import system.dispatcher

  def control(cmd: ControlCommand): Future[Respond] = {
    import akka.pattern.ask
    implicit val to: Timeout = 3.seconds
    (actor ? cmd).mapTo[Respond]
  }

  def request(req: String): Future[Either[String, Respond]] = {
    import akka.pattern.ask
    implicit val to: Timeout = 2.seconds
    val resp = (actor ? Request(req)).mapTo[Respond]
    resp.map(Right(_))
  }

  def stop(delay: Option[Int] = None): Future[Unit] = {
    val promise = Promise[Unit]()

    cluster.registerOnMemberRemoved {
      delay.foreach(Thread.sleep(_))
      actor ! PoisonPill
      system.terminate()
      promise.trySuccess(())
    }
    cluster.leave(cluster.selfAddress)

    promise.future
  }
}
