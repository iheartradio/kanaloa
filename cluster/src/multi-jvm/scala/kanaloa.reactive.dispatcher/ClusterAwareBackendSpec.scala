package kanaloa.reactive.dispatcher


import org.scalatest.{Matchers, BeforeAndAfterAll}

import language.postfixOps
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.PoisonPill
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeSpecCallbacks, MultiNodeConfig, MultiNodeSpec}
import akka.testkit._
import akka.actor.ActorLogging
import org.scalatest.{ BeforeAndAfterAll, WordSpecLike }
import org.scalatest.Matchers

object ClusterAwareBackendSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
    akka.remote.log-remote-lifecycle-events = off
    akka.cluster.auto-down-unreachable-after = 0s
                                         """))



}


class ClusterAwareBackendSpecMultiJvmNode1 extends ClusterAwareBackendSpec
class ClusterAwareBackendSpecMultiJvmNode2 extends ClusterAwareBackendSpec
class ClusterAwareBackendSpecMultiJvmNode3 extends ClusterAwareBackendSpec

class ClusterAwareBackendSpec extends  MultiNodeSpec(ClusterAwareBackendSpec) with STMultiNodeSpec with ImplicitSender {
  import ClusterAwareBackendSpec._

  override def initialParticipants = roles.size

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      Cluster(system) join node(to).address
    }
    enterBarrier(from.name + "-joined")
  }

  "A ClusterAwareBackend" must {

    "startup 2 node cluster" in within(15 seconds) {
      join(first, first)
      join(second, first)
      enterBarrier("after-1")
    }
  }
}
