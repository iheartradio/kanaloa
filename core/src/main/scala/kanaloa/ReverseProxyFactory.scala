package kanaloa

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorRef, ActorSystem}
import akka.util.Timeout
import com.typesafe.config.{ConfigFactory, Config}
import kanaloa.ApiProtocol.{ShutdownForcefully, ShutdownSuccessfully, ShutdownGracefully}
import kanaloa.ReverseProxy.ShutdownException
import kanaloa.handler.{HandlerProviderAdaptor, HandlerProvider, Handler, SimpleFunctionHandler}
import kanaloa.metrics.StatsDClient

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

trait ReverseProxy[TReq, TResp] extends (TReq ⇒ Future[Either[WorkException, TResp]]) {
  def name: String
  def close(): Future[Unit]
}

object ReverseProxy {
  class ShutdownException(msg: String) extends Exception(msg)
}

class ReverseProxyFactory private (config: Config) {
  private val index = new AtomicInteger(0)
  private implicit val system: ActorSystem = ActorSystem("kanaloa-reverse-proxy-system", config)
  private implicit val statsDClient: Option[StatsDClient] = StatsDClient(config)

  def apply[TReq, TResp](f: TReq ⇒ Future[TResp], name: String): ReverseProxy[TReq, TResp] = {
    import system.dispatcher //the system dispatcher is an appropriate execution context for the SimpleFunctionHandler
    val handler = HandlerProviderAdaptor.fromSimpleFunction(system.dispatcher)(f)
    val dispatcherRef = system.actorOf(PushingDispatcher.safeProps(name, handler, config), "kanaloa-reverse-proxy-" + name)
    new ReverseProxyImpl[TReq, TResp](name, dispatcherRef)
  }

  def apply[TReq, TResp](f: TReq ⇒ Future[TResp]): ReverseProxy[TReq, TResp] = {
    apply(f, s"Anonymous-Proxy-${index.incrementAndGet()}")
  }

  def close(): Unit = system.terminate()
}

object ReverseProxyFactory {
  /**
   * Creates a new [[kanaloa.ReverseProxyFactory]] with all the heavy dependency.
   * Ideally this should be a singleton instance.
   */
  def apply(cfg: Config = ConfigFactory.load()): ReverseProxyFactory = {
    new ReverseProxyFactory(cfg)
  }
}

private[kanaloa] class ReverseProxyImpl[TReq, TResp](val name: String, dispatcherRef: ActorRef)(implicit ex: ExecutionContext) extends ReverseProxy[TReq, TResp] {
  import akka.pattern.ask
  implicit val to: Timeout = 24.hour //this should never be used, since the dispatcher controls the latency.

  override def apply(req: TReq): Future[Either[WorkException, TResp]] = (dispatcherRef ? req).map {
    case ex: WorkException      ⇒ Left(ex)
    case resp: TResp @unchecked ⇒ Right(resp) //the handler API ensures the return type.
  }

  override def close(): Future[Unit] = (dispatcherRef ? ShutdownGracefully) map {
    case ShutdownSuccessfully ⇒ ()
    case ShutdownForcefully   ⇒ throw new ShutdownException("Shutdown forcefully with possible work lost.")
  }
}
