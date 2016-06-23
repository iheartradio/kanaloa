package kanaloa.reactive.dispatcher


import akka.routing.{GetRoutees, Routees}
import akka.util.Timeout
import org.scalatest.{Matchers, BeforeAndAfterAll}

import language.postfixOps
import scala.concurrent.Await
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.PoisonPill
import akka.actor.Props
import akka.cluster.{ClusterSpec, Cluster}
import akka.cluster.ClusterEvent._
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeSpecCallbacks, MultiNodeConfig, MultiNodeSpec}
import akka.testkit._
import akka.actor.ActorLogging
import org.scalatest.{ BeforeAndAfterAll, WordSpecLike }
import org.scalatest.Matchers
import akka.pattern.ask

object ClusterAwareBackendSpec extends ClusterConfig {

  val first = clusterNode("first")
  val second = clusterNode("second")
  val third = clusterNode("third")

  commonConfig(clusterConfig)
  testTransport(on = true)
}


class ClusterAwareBackendSpecMultiJvmNode1 extends ClusterAwareBackendSpec
class ClusterAwareBackendSpecMultiJvmNode2 extends ClusterAwareBackendSpec
class ClusterAwareBackendSpecMultiJvmNode3 extends ClusterAwareBackendSpec

case class EchoMessage(i: Int)
class ClusterAwareBackendSpec extends  MultiNodeSpec(ClusterAwareBackendSpec) with ClusterSpec with ImplicitSender {
  import ClusterAwareBackendSpec._

  override def initialParticipants = roles.size
  implicit val timeout: Timeout = 30.seconds

  def currentRoutees(router: ActorRef) =
    Await.result(router ? GetRoutees, 30.seconds).asInstanceOf[Routees].routees

  "A ClusterAwareBackend" must {

    "startup 2 node cluster" in within(15 seconds) {
      join(first, first)
      join(second, first)
      enterBarrier("after-1")
    }

    "send messages through" in within(5 seconds) {

      runOn(third) {
        enterBarrier("deployed")
      }

      runOn(second) {
        system.actorOf(TestActors.echoActorProps, "echo")
        enterBarrier("deployed")
      }

      runOn(first) {
        enterBarrier("deployed")
        val backend = ClusterAwareBackend("echo", second.name)
        val dispatcher = system.actorOf(PushingDispatcher.props(
          name = "test",
          backend
        )(ResultChecker.simple[EchoMessage]))

        dispatcher ! EchoMessage(1)
        expectMsg(EchoMessage(1))

      }

      enterBarrier("testOne")
    }

  }
}
