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
  import system.{dispatcher => ex}
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

//   todo: right now kanaloa uses the first available handler to initialize the entire worker pool. this test can be easier after we implement the one queue processor per handler
//    "slow routees doesn't block dispatcher" in within(15 seconds) {
//
//      awaitClusterUp(first, second, third)
//
//      val servicePath = "service2"
//
//
//      runOn(third) {
//        system.actorOf(TestActors.echoActorProps, servicePath) //a supper fast service
//      }
//
//      runOn(second) {
//        val prob: TestProbe = TestProbe()
//        system.actorOf(TestActors.forwardActorProps(prob.ref), servicePath) //unresponsive service
//        prob.expectMsg(EchoMessage(1))
//      }
//
//      runOn(first) {
//
//        val backend: HandlerProvider[Any] = new ClusterAwareHandlerProvider(servicePath, serviceClusterRole)(ResultChecker.expectType[EchoMessage])
//        val dispatcher = system.actorOf(PushingDispatcher.props(
//          name = "test",
//          backend,
//          kanaloaConfig
//        ))
//
//        (1 to 100).foreach { _ =>
//          dispatcher ! EchoMessage(1)
//        }
//
//        receiveN(99).foreach { m =>
//           m should ===(EchoMessage(1))
//        }
//
//
//      }
//
//      enterBarrier("second-finished")
//    }

  }
}
