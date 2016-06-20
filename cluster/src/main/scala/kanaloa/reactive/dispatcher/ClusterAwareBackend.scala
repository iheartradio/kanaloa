package kanaloa.reactive.dispatcher

import java.util.UUID

import akka.actor.{ActorSystem, Props, ActorRef, ActorRefFactory}
import akka.cluster.routing.{ClusterRouterGroup, ClusterRouterGroupSettings}
import akka.routing._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.Future
import scala.concurrent.duration._

class ClusterAwareBackend(actorRefPath: String, role: String)(implicit system: ActorSystem) extends Backend {

  private val routingLogic: RoutingLogic = RoundRobinRoutingLogic()

  private lazy val router: ActorRef = {
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
    import f.dispatcher
    (router ? GetRoutees).map {
      case Routees(routees) ⇒ routingLogic.select((), routees)
    }
  }

}
