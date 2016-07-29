package kanaloa.reactive.dispatcher

import java.util.UUID

import akka.actor._
import akka.cluster.routing.{ClusterRouterGroup, ClusterRouterGroupSettings}
import akka.routing._
import akka.pattern.ask
import akka.util.Timeout
import kanaloa.reactive.dispatcher.ClusterAwareBackend.{FailedToGetRemoteRoutee, RouteeRef}
import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * A [[Backend]] that represent actor deployed on remote cluster members.
 *
 * @param actorRefPath the path with which the actor is deployed
 * @param role the role of the cluster member on which the actor is deployed.
 * @param routingLogic routing logic for routing between multiple remote actor deployments
 * @param maxNumberOfBackendNodes the maximum number of deployments
 * @param system
 * @param timeout timeout before the backend fails to retrieve an actual remote actorRef for kanaloa worker to work with
 */
class ClusterAwareBackend(
  actorRefPath:            String,
  role:                    String,
  routingLogic:            RoutingLogic = RoundRobinRoutingLogic(),
  maxNumberOfBackendNodes: Int          = 100
)(implicit
  system: ActorSystem,
  timeout: Timeout) extends Backend {

  private[dispatcher] lazy val router: ActorRef = {

    val path = "/user/" + actorRefPath.stripPrefix("/user/")
    val routerProps: Props = ClusterRouterGroup(
      RoundRobinGroup(List(path)),
      ClusterRouterGroupSettings(
        totalInstances = maxNumberOfBackendNodes,
        routeesPaths = List(path),
        allowLocalRoutees = false, useRole = Some(role)
      )
    ).props()
    val prefix = s"clusterAwareBackendInternalRouter-$path-$role-"
    system.actorOf(routerProps, prefix.replace('/', '-') + UUID.randomUUID().toString)
  }

  override def apply(f: ActorRefFactory): Future[ActorRef] = {
    val retriever = f.actorOf(ClusterAwareBackend.retrieverProps(router, routingLogic))
    import system.dispatcher
    //let the retriever handle the timeout internally.
    retriever.ask(ClusterAwareBackend.GetRoutee)(timeout = timeout.duration * 2).map {
      case RouteeRef(actorRef) ⇒ actorRef
      case err                 ⇒ throw new FailedToGetRemoteRoutee(err.toString)
    }
  }

}

object ClusterAwareBackend {

  case class FailedToGetRemoteRoutee(msg: String) extends Exception("Failed to retrieve router due to " + msg)
  /**
   * Creates a [[ClusterAwareBackend]]
   * @param actorRefPath path of the remote actor
   * @param role role of the node on which the remote actor is deployed
   * @param system the cluster enabled [[ActorSystem]]
   * @param timeout for finding the actual remote actor, which means both current node and remote node has to be in the cluster.
   */
  def apply(actorRefPath: String, role: String)(implicit system: ActorSystem, timeout: Timeout): ClusterAwareBackend = new ClusterAwareBackend(actorRefPath, role)

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

  private def retrieverProps(router: ActorRef, routingLogic: RoutingLogic)(implicit timeout: Timeout) = Props(new RouteeRetriever(router, routingLogic)).withDeploy(Deploy.local)
}
