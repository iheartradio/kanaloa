package akka.cluster

import java.util.concurrent.ConcurrentHashMap

import akka.actor.{ActorIdentity, ActorSystem, Address}
import akka.remote.testconductor.Controller.ClientDisconnected
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeSpec
import akka.testkit.EventFilter
import akka.testkit.TestEvent.Mute
import kanaloa.reactive.dispatcher.STMultiNodeSpec
import scala.concurrent.duration._
import scala.language.implicitConversions
//adapted from https://github.com/akka/akka/blob/master/akka-cluster/src/multi-jvm/scala/akka/cluster/MultiNodeClusterSpec.scala
trait ClusterSpec extends  STMultiNodeSpec { self: MultiNodeSpec =>


  override def initialParticipants = roles.size
  override def atStartup(): Unit = {
    muteLog()
  }

  def muteLog(sys: ActorSystem = system): Unit = {
    if (!sys.log.isDebugEnabled) {
      Seq(
        ".*Metrics collection has started successfully.*",
        ".*Metrics will be retreived from MBeans.*",
        ".*Cluster Node.* - registered cluster JMX MBean.*",
        ".*Cluster Node.* - is starting up.*",
        ".*Shutting down cluster Node.*",
        ".*Cluster node successfully shut down.*",
        ".*Using a dedicated scheduler for cluster.*") foreach { s ⇒
        sys.eventStream.publish(Mute(EventFilter.info(pattern = s)))
      }

      muteDeadLetters(
        classOf[ActorIdentity],
        classOf[ClientDisconnected],
        classOf[ClusterHeartbeatSender.Heartbeat],
        classOf[ClusterHeartbeatSender.HeartbeatRsp],
        classOf[GossipEnvelope],
        classOf[GossipStatus],
        classOf[MetricsGossipEnvelope],
        classOf[ClusterEvent.ClusterMetricsChanged],
        classOf[InternalClusterAction.Tick],
        classOf[akka.actor.PoisonPill],
        classOf[akka.dispatch.sysmsg.DeathWatchNotification],
        classOf[akka.remote.transport.AssociationHandle.Disassociated],
        //        akka.remote.transport.AssociationHandle.Disassociated.getClass,
        classOf[akka.remote.transport.ActorTransportAdapter.DisassociateUnderlying],
        //        akka.remote.transport.ActorTransportAdapter.DisassociateUnderlying.getClass,
        classOf[akka.remote.transport.AssociationHandle.InboundPayload])(sys)

    }
  }


  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      Cluster(system) join node(to).address
    }
    enterBarrier(from.name + "-joined")
  }

  private val cachedAddresses = new ConcurrentHashMap[RoleName, Address]

  /**
    * Lookup the Address for the role.
    *
    * Implicit conversion from RoleName to Address.
    *
    * It is cached, which has the implication that stopping
    * and then restarting a role (jvm) with another address is not
    * supported.
    */
  implicit def address(role: RoleName): Address = {
    cachedAddresses.get(role) match {
      case null ⇒
        val address = node(role).address
        cachedAddresses.put(role, address)
        address
      case address ⇒ address
    }
  }


  def clusterView: ClusterReadView = cluster.readView

  /**
    * Get the cluster node to use.
    */
  def cluster: Cluster = Cluster(system)

  /**
    * Use this method for the initial startup of the cluster node.
    */
  def startClusterNode(): Unit = {
    if (clusterView.members.isEmpty) {
      cluster join myself
      awaitAssert(clusterView.members.map(_.address) should contain(address(myself)))
    } else
      clusterView.self
  }

  /**
    * Initialize the cluster of the specified member
    * nodes (roles) and wait until all joined and `Up`.
    * First node will be started first  and others will join
    * the first.
    */
  def awaitClusterUp(roles: RoleName*): Unit = {
    runOn(roles.head) {
      // make sure that the node-to-join is started before other join
      startClusterNode()
    }
    enterBarrier(roles.head.name + "-started")
    if (roles.tail.contains(myself)) {
      cluster.join(roles.head)
    }
    if (roles.contains(myself)) {
      awaitMembersUp(numberOfMembers = roles.length)
    }
    enterBarrier(roles.map(_.name).mkString("-") + "-joined")
  }

  /**
    * Wait until the expected number of members has status Up has been reached.
    * Also asserts that nodes in the 'canNotBePartOfMemberRing' are *not* part of the cluster ring.
    */
  def awaitMembersUp(
                      numberOfMembers:          Int,
                      canNotBePartOfMemberRing: Set[Address]   = Set.empty,
                      timeout:                  FiniteDuration = 25.seconds): Unit = {
    within(timeout) {
      if (!canNotBePartOfMemberRing.isEmpty) // don't run this on an empty set
        awaitAssert(canNotBePartOfMemberRing foreach (a ⇒ clusterView.members.map(_.address) should not contain (a)))
      awaitAssert(clusterView.members.size should ===(numberOfMembers))
      awaitAssert(clusterView.members.map(_.status) should ===(Set(MemberStatus.Up)))
      // clusterView.leader is updated by LeaderChanged, await that to be updated also
      val expectedLeader = clusterView.members.headOption.map(_.address)
      awaitAssert(clusterView.leader should ===(expectedLeader))
    }
  }

  def joinWithin(joinNode: RoleName, max: Duration = remainingOrDefault, interval: Duration = 1.second): Unit = {
    def memberInState(member: Address, status: Seq[MemberStatus]): Boolean =
      clusterView.members.exists { m ⇒ (m.address == member) && status.contains(m.status) }

    cluster join joinNode
    awaitCond({
      clusterView.refreshCurrentState()
      if (memberInState(joinNode, List(MemberStatus.up)) &&
        memberInState(myself, List(MemberStatus.Joining, MemberStatus.Up)))
        true
      else {
        cluster join joinNode
        false
      }
    }, max, interval)
  }

}
