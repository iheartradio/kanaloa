package kanaloa

import java.util.UUID

import akka.actor.Actor.Receive
import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster.MemberStatus.Up
import akka.cluster.{Member, Cluster}
import akka.cluster.routing.{ClusterRouterGroup, ClusterRouterGroupSettings}
import akka.pattern.ask
import akka.routing._
import akka.util.Timeout
import kanaloa.handler.GeneralActorRefHandler.ResultChecker
import kanaloa.handler.{Handler, GeneralActorRefHandler, AgentHandlerProvider, HandlerProvider}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import ClusterAwareHandlerProvider._
import util.Naming._
/**
 * A [[kanaloa.handler.HandlerProvider]] that represent actor deployed on remote cluster members.
 *
 * @param actorRefPath the path with which the actor is deployed
 * @param role the role of the cluster member on which the actor is deployed.
 * @param timeout timeout before the backend fails to retrieve an actual remote actorRef for kanaloa worker to work with
 */
class ClusterAwareHandlerProvider[TResp, TError](
  actorRefPath:    String,
  role:            String,
  includeWeaklyUp: Boolean = false
)(resultChecker: ResultChecker[TResp, TError])(
  implicit
  arf:     ActorRefFactory,
  timeout: Timeout,
  val ex:  ExecutionContext
) extends AgentHandlerProvider[Any] {
  val path = "/user/" + actorRefPath.stripPrefix("/user/")

  arf.actorOf(monitorProps(
    onMemberAdded,
    onMemberRemoved,
    includeWeaklyUp
  ), sanitizeActorName(s"cluster-monitor-for-actor-ref-$actorRefPath-on-$role-${UUID.randomUUID()}"))

  def onMemberAdded(member: Member): Unit = {
    if (findHandlerBy(member).isEmpty) {
      newHandler(member).foreach { h ⇒
        if (findHandlerBy(member).isEmpty) //double check.
          addHandler(h)
      }
    }
  }

  def findHandlerBy(member: Member): Option[GeneralActorRefHandler[TResp, TError]] = handlers.collectFirst {
    case handler: GeneralActorRefHandler[TResp, TError] if handler.actor.path.address == member.address ⇒
      handler
  }

  def onMemberRemoved(member: Member): Unit = {
    findHandlerBy(member) foreach removeHandler
  }

  def newHandler(member: Member): Future[Handler[Any]] =
    arf.actorSelection(member.address.toString + path).resolveOne().map {
      GeneralActorRefHandler(member.address.hostPort, _, arf)(resultChecker)
    }

}

object ClusterAwareHandlerProvider {
  def monitorProps(onMemberAdded: Member ⇒ Unit, onMemberRemoved: Member ⇒ Unit, includeWeaklyUp: Boolean = false): Props = Props(new ClusterMonitorActor(onMemberAdded, onMemberRemoved, includeWeaklyUp))

  private class ClusterMonitorActor(onMemberAdded: Member ⇒ Unit, onMemberRemoved: Member ⇒ Unit, includeWeaklyUp: Boolean = false) extends Actor {
    val cluster = Cluster(context.system)

    val downEvents = List(classOf[UnreachableMember], classOf[MemberRemoved], classOf[MemberExited])
    val upEvents = classOf[MemberUp] :: classOf[ReachableMember] :: (if (includeWeaklyUp) List(classOf[MemberWeaklyUp]) else Nil)
    cluster.subscribe(self, (downEvents ++ upEvents): _*)

    override def receive: Receive = {
      case state: CurrentClusterState ⇒
        state.members.filter(_.status == Up).foreach(onMemberAdded)
      case MemberUp(m)          ⇒ onMemberAdded(m)
      case MemberWeaklyUp(m)    ⇒ onMemberAdded(m)
      case ReachableMember(m)   ⇒ onMemberAdded(m)
      case UnreachableMember(m) ⇒ onMemberRemoved(m)
      case MemberRemoved(m, _)  ⇒ onMemberRemoved(m)
      case MemberExited(m)      ⇒ onMemberRemoved(m)
    }
  }

}
