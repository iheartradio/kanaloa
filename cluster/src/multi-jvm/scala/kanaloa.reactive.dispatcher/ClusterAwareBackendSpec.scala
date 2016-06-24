package kanaloa.reactive.dispatcher


import akka.actor._
import akka.cluster.ClusterSpec
import akka.pattern.ask
import akka.remote.testkit.MultiNodeSpec
import akka.routing.{GetRoutees, Routees}
import akka.testkit._
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

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


  val kanaloaConfig = ConfigFactory.parseString(
    """
      |kanaloa {
      |  default-dispatcher {
      |    workerPool.startingPoolSize = 3
      |  }
      |}
    """.stripMargin)


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
          backend,
          kanaloaConfig
        )(ResultChecker.simple[EchoMessage]))

        dispatcher ! EchoMessage(1)
        expectMsg(EchoMessage(1))

      }

      enterBarrier("testOne")
    }

  }
}
