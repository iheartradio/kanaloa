package kanaloa.dispatcher


import akka.actor._
import akka.cluster.ClusterSpec
import akka.pattern.ask
import akka.remote.testkit.MultiNodeSpec
import akka.routing.{GetRoutees, Routees}
import akka.testkit._
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import kanaloa.dispatcher.ClusterAwareBackendSpec._
import kanaloa.dispatcher.metrics.StatsDClient

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

object ClusterAwareBackendSpec extends ClusterConfig {
  val serviceClusterRole = "service"
  val first = clusterNode("first")
  val second = clusterNode("second", serviceClusterRole)
  val third = clusterNode("third", serviceClusterRole)

  commonConfig(clusterConfig)
  testTransport(on = true)
  implicit val timeout: Timeout = 30.seconds



  val kanaloaConfig = ConfigFactory.parseString(
    """
      |kanaloa {
      |  default-dispatcher {
      |    worker-pool.starting-pool-size = 2
      |  }
      |}
    """.stripMargin)

}


class ClusterAwareBackendSpecMultiJvmNode1 extends ClusterAwareBackendSpec
class ClusterAwareBackendSpecMultiJvmNode2 extends ClusterAwareBackendSpec
class ClusterAwareBackendSpecMultiJvmNode3 extends ClusterAwareBackendSpec

case class EchoMessage(i: Int)

class ClusterAwareBackendSpec extends  MultiNodeSpec(ClusterAwareBackendSpec) with ClusterSpec with ImplicitSender {
  import ClusterAwareBackendSpec._
  implicit val noStatsD: Option[StatsDClient] = None

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
        system.actorOf(TestActors.echoActorProps, "echoService")
        enterBarrier("deployed")
      }

      runOn(first) {
        enterBarrier("deployed")
        val backend = ClusterAwareBackend("echoService", serviceClusterRole)
        val dispatcher = system.actorOf(PushingDispatcher.props(
          name = "test",
          backend,
          kanaloaConfig
        )(ResultChecker.expectType[EchoMessage]))

        dispatcher ! EchoMessage(1)
        expectMsg(EchoMessage(1))

      }

      enterBarrier("testOne")
    }


    "slow routees doesn't block dispatcher" in within(15 seconds) {

      awaitClusterUp(first, second, third)

      val servicePath = "service2"


      runOn(third) {
        val prob: TestProbe = TestProbe()
        system.actorOf(TestActors.forwardActorProps(prob.ref), servicePath) //unresponsive service

        enterBarrier("service2-deployed")
        enterBarrier("all-message-replied")
        prob.expectMsg(EchoMessage(1))
      }

      runOn(second) {
        system.actorOf(TestActors.echoActorProps, servicePath) //a supper fast service
        enterBarrier("service2-deployed")
        enterBarrier("all-message-replied")

      }

      runOn(first) {
        enterBarrier("service2-deployed")
        val backend = ClusterAwareBackend(servicePath, serviceClusterRole)
        val dispatcher = system.actorOf(PushingDispatcher.props(
          name = "test",
          backend,
          kanaloaConfig
        )(ResultChecker.expectType[EchoMessage]))

        (1 to 100).foreach { _ =>
          dispatcher ! EchoMessage(1)
        }

        receiveN(99).foreach { m =>
           m should ===(EchoMessage(1))
        }
        enterBarrier("all-message-replied")

      }

      enterBarrier("second-finished")
    }

  }
}
