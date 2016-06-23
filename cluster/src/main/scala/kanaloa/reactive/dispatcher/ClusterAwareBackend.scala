package kanaloa.reactive.dispatcher

import java.util.UUID

import akka.actor._
import akka.cluster.routing.{ClusterRouterGroup, ClusterRouterGroupSettings}
import akka.routing._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.Future
import scala.concurrent.duration._

class ClusterAwareBackend(
  actorRefPath: String,
  role:         String,
  routingLogic: RoutingLogic = RoundRobinRoutingLogic()
)(implicit system: ActorSystem) extends Backend {

  private[dispatcher] lazy val router: ActorRef = {
    val routerProps: Props = ClusterRouterGroup(
      RoundRobinGroup(List("/user/" + actorRefPath)),
      ClusterRouterGroupSettings(
        totalInstances = 100,
        routeesPaths = List("/user/" + actorRefPath),
        allowLocalRoutees = false, useRole = Some(role)
      )
    ).props()
    system.actorOf(routerProps, "clusterAwareBackendInternalRouter-$path-$role-" + UUID.randomUUID().toString)
  }

  override def apply(f: ActorRefFactory): Future[Routee] = {
    implicit val to: Timeout = 1.seconds //this should be instant according to the implementation
    val retriever = f.actorOf(ClusterAwareBackend.retrieverProps(router, routingLogic))
    (retriever ? ClusterAwareBackend.GetRoutee).mapTo[Routee]
  }

}

object ClusterAwareBackend {
  def apply(actorRefPath: String, role: String)(implicit system: ActorSystem): ClusterAwareBackend = new ClusterAwareBackend(actorRefPath, role)

  private class RouteeRetriever(router: ActorRef, routingLogic: RoutingLogic)(implicit timeout: Timeout) extends Actor {
    import context.dispatcher
    def receive = {

      case GetRoutee ⇒
        router ! GetRoutees
        val timeoutCancellable = context.system.scheduler.scheduleOnce(timeout.duration, self, TimedOut)
        context become waitForRoutees(sender, timeoutCancellable)
    }

    def waitForRoutees(replyTo: ActorRef, timeoutCancellable: Cancellable, retryWait: FiniteDuration = 50.milliseconds): Receive = {
      case Routees(routees) if routees.length > 0 ⇒
        replyTo ! routingLogic.select((), routees)
        context stop self
      case Routees(empty) ⇒
        context.system.scheduler.scheduleOnce(retryWait, router, GetRoutees)
        context become waitForRoutees(replyTo, timeoutCancellable, retryWait * 2)

      case TimedOut ⇒
        replyTo ! TimedOut
        context stop self
    }
  }

  private case object GetRoutee
  private case object TimedOut

  private def retrieverProps(router: ActorRef, routingLogic: RoutingLogic)(implicit timeout: Timeout) = Props(new RouteeRetriever(router, routingLogic))
}
