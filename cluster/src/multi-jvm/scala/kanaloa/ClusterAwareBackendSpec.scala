package kanaloa


import akka.actor._
import akka.cluster.ClusterSpec
import akka.pattern.ask
import akka.remote.testkit.MultiNodeSpec
import akka.routing.{GetRoutees, Routees}
import akka.testkit._
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import kanaloa.ClusterAwareBackendSpec._
import kanaloa.handler.{HandlerProvider, ResultChecker}
import kanaloa.metrics.StatsDClient
import org.scalatest.Tag

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

case class EchoMessage(i: Int)

abstract class ClusterAwareBackendSpecBase extends  MultiNodeSpec(ClusterAwareBackendSpec) with ClusterSpec with ImplicitSender {

  import ClusterAwareBackendSpec._

  implicit val noStatsD: Option[StatsDClient] = None

  implicit val ex = system.dispatcher

}

class ClusterAwareBackendBasicSpecMultiJvmNode1 extends ClusterAwareBackendBasicSpec
class ClusterAwareBackendBasicSpecMultiJvmNode2 extends ClusterAwareBackendBasicSpec
class ClusterAwareBackendBasicSpecMultiJvmNode3 extends ClusterAwareBackendBasicSpec


class ClusterAwareBackendBasicSpec extends ClusterAwareBackendSpecBase {
  "A ClusterAwareBackend" must {

    "startup 2 node cluster" in within(15 seconds) {
      join(first, first)
      join(second, first)
      enterBarrier("after-1")
    }

    "send messages through" in within(5 seconds) {
      runOn(second) {
        system.actorOf(TestActors.echoActorProps, "echoService")
      }

      enterBarrier("deployed")

      runOn(first) {
        val backend: HandlerProvider[Any] =
          new ClusterAwareHandlerProvider("echoService", serviceClusterRole)((ResultChecker.expectType[EchoMessage]))

        val dispatcher = system.actorOf(PushingDispatcher.props(
          name = "test",
          backend,
          kanaloaConfig
        ))

        dispatcher ! EchoMessage(1)
        expectMsg(EchoMessage(1))

      }

      enterBarrier("testOne")
    }
  }
}


class ClusterAwareBackendLoadBalanceSpecMultiJvmNode1 extends ClusterAwareBackendLoadBalanceSpec
class ClusterAwareBackendLoadBalanceSpecMultiJvmNode2 extends ClusterAwareBackendLoadBalanceSpec
class ClusterAwareBackendLoadBalanceSpecMultiJvmNode3 extends ClusterAwareBackendLoadBalanceSpec

class ClusterAwareBackendLoadBalanceSpec extends ClusterAwareBackendSpecBase {
  "A ClusterAwareBackend" must {
    "slow routees doesn't block dispatcher" taggedAs (Tag("loadbalancing")) in within(15 seconds) {

      awaitClusterUp(first, second, third)

      val servicePath = "service2"


      runOn(third) {
        system.actorOf(TestActors.echoActorProps, servicePath) //a supper fast service
      }

      runOn(second) {
        val prob: TestProbe = TestProbe()
        system.actorOf(TestActors.forwardActorProps(prob.ref), servicePath) //unresponsive service
        prob.expectMsg(EchoMessage(1))
      }

      runOn(first) {

        val backend: HandlerProvider[Any] = new ClusterAwareHandlerProvider(servicePath, serviceClusterRole)(ResultChecker.expectType[EchoMessage])
        val dispatcher = system.actorOf(PushingDispatcher.props(
          name = "test",
          backend,
          kanaloaConfig
        ))

        (1 to 100).foreach { _ =>
          dispatcher ! EchoMessage(1)
        }

        receiveN(98).foreach { m =>  //the slow worker pool has two workers, which took two requests
           m should ===(EchoMessage(1))
        }


      }

      enterBarrier("second-finished")
    }

  }
}
