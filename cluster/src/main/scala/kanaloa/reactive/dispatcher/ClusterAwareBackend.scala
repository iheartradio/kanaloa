package kanaloa.reactive.dispatcher

import java.util.UUID

import akka.actor._
import akka.cluster.routing.{ClusterRouterGroup, ClusterRouterGroupSettings}
import akka.routing._
import akka.pattern.ask
import akka.util.Timeout
import kanaloa.reactive.dispatcher.ClusterAwareBackend.RouteeRef
import scala.concurrent.Future
import scala.concurrent.duration._

class ClusterAwareBackend(
  actorRefPath: String,
  role:         String,
  routingLogic: RoutingLogic = RoundRobinRoutingLogic()
)(implicit
  system: ActorSystem,
  timeout: Timeout = 1.seconds) extends Backend {

  private[dispatcher] lazy val router: ActorRef = {
    val routerProps: Props = ClusterRouterGroup(
      RoundRobinGroup(List("/user/" + actorRefPath)),
      ClusterRouterGroupSettings(
        totalInstances = 100,
        routeesPaths = List("/user/" + actorRefPath),
        allowLocalRoutees = false, useRole = Some(role)
      )
    ).props()
    system.actorOf(routerProps, s"clusterAwareBackendInternalRouter-$actorRefPath-$role-" + UUID.randomUUID().toString)
  }

  override def apply(f: ActorRefFactory): Future[ActorRef] = {
    val retriever = f.actorOf(ClusterAwareBackend.retrieverProps(router, routingLogic))
    import system.dispatcher
    (retriever ? ClusterAwareBackend.GetRoutee).mapTo[RouteeRef].map(_.actorRef)
  }

}

object ClusterAwareBackend {
  def apply(actorRefPath: String, role: String)(implicit system: ActorSystem): ClusterAwareBackend = new ClusterAwareBackend(actorRefPath, role)

  private class RouteeRetriever(router: ActorRef, routingLogic: RoutingLogic)(implicit timeout: Timeout) extends Actor {
    import context.dispatcher

    var timeoutCancellable: Option[Cancellable] = None

    override def postStop(): Unit = {
      timeoutCancellable.foreach(_.cancel())
    }

    def receive = {

      case GetRoutee ⇒
        router ! GetRoutees
        timeoutCancellable = Some(context.system.scheduler.scheduleOnce(timeout.duration, self, TimedOut))
        context become waitForRoutees(sender)
    }

    def waitForRoutees(replyTo: ActorRef, retryWait: FiniteDuration = 50.milliseconds): Receive = {
      case Routees(routees) if routees.length > 0 ⇒

        val routee = routingLogic.select((), routees)
        val actorRefF = routee match {
          case ActorSelectionRoutee(as) ⇒ as.resolveOne(timeout.duration)
          case ActorRefRoutee(ar)       ⇒ Future.successful(ar)
        }
        actorRefF.foreach { ref ⇒
          replyTo ! RouteeRef(ref)
          context stop self
        }

      case Routees(empty) ⇒
        context.system.scheduler.scheduleOnce(retryWait, router, GetRoutees)
        context become waitForRoutees(replyTo, retryWait * 2)

      case TimedOut ⇒
        replyTo ! TimedOut
        context stop self
    }
  }

  private case object GetRoutee
  private case class RouteeRef(actorRef: ActorRef)
  private case object TimedOut

  private def retrieverProps(router: ActorRef, routingLogic: RoutingLogic)(implicit timeout: Timeout) = Props(new RouteeRetriever(router, routingLogic))
}
