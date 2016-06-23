package akka.cluster

import java.util.concurrent.ConcurrentHashMap

import akka.actor.{ActorSystem, Address}
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeSpec
import akka.testkit.EventFilter
import akka.testkit.TestEvent.Mute
import kanaloa.reactive.dispatcher.STMultiNodeSpec
import scala.concurrent.duration._

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
}
