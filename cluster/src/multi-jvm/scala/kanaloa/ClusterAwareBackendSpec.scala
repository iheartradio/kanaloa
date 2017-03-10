package kanaloa


import akka.actor.Actor.Receive
import akka.actor._
import akka.cluster.ClusterSpec
import akka.pattern.ask
import akka.remote.testkit.MultiNodeSpec
import akka.routing.{GetRoutees, Routees}
import akka.testkit._
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import kanaloa.ClusterAwareBackendLoadBalance2Spec.DelayedEcho
import kanaloa.ClusterAwareBackendSpec._
import kanaloa.handler.{HandlerProvider, ResultChecker}
import kanaloa.metrics.StatsDClient
import kanaloa.util.MessageScheduler
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
      |  initial-grace-period = 20s #cluster handler needs more time to boot up.
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

        receiveN(98, 30.seconds).foreach { m =>  //the slow worker pool has two workers, which took two requests
           m should ===(EchoMessage(1))
        }


      }

      enterBarrier("finished")
    }

  }
}
class ClusterAwareBackendLoadBalance2SpecMultiJvmNode1 extends ClusterAwareBackendLoadBalance2Spec
class ClusterAwareBackendLoadBalance2SpecMultiJvmNode2 extends ClusterAwareBackendLoadBalance2Spec
class ClusterAwareBackendLoadBalance2SpecMultiJvmNode3 extends ClusterAwareBackendLoadBalance2Spec

class ClusterAwareBackendLoadBalance2Spec extends ClusterAwareBackendSpecBase {
  "A ClusterAwareBackend" must {
    "allow instance to leave cluster without losing requests" taggedAs (Tag("loadBalancing")) in within(15 seconds) {

      awaitClusterUp(first, second, third)

      val servicePath = "service2"


      runOn(third) {
        system.actorOf(Props(classOf[DelayedEcho]), servicePath) //a supper fast service
        enterBarrier("servicesStarted")
        enterBarrier("first100messages")
      }

      runOn(second) {
        val service = system.actorOf(Props(classOf[DelayedEcho]), servicePath) //a supper fast service
        enterBarrier("servicesStarted")
        enterBarrier("first100messages")
        cluster leave myself
        watch(service)
        Thread.sleep(700) //wait for the leaveto take effect before killing self.
        service ! PoisonPill
        expectTerminated(service)
      }


      runOn(first) {
        enterBarrier("servicesStarted")
        val backend: HandlerProvider[Any] = new ClusterAwareHandlerProvider(servicePath, serviceClusterRole)(ResultChecker.expectType[EchoMessage])
        val dispatcher = system.actorOf(PushingDispatcher.props(
          name = "test",
          backend,
          kanaloaConfig
        ))

        val numOfMessagesBeforeOneNodeLeaving = 50
        val numOfMessagesAfterOneNodeLeaving = 150
        val totalMsgs = numOfMessagesAfterOneNodeLeaving + numOfMessagesBeforeOneNodeLeaving

        def sendMessages(num: Int): Unit = {
          (1 to num).foreach { _ =>
            Thread.sleep(10)
            dispatcher ! EchoMessage(1)
          }
        }

        sendMessages( numOfMessagesBeforeOneNodeLeaving )

        enterBarrier("first100messages")

        sendMessages( numOfMessagesAfterOneNodeLeaving )


        val (succeeds, failures) = receiveN(totalMsgs).partition(
          _ == EchoMessage(1)
        )

        withClue(s"${failures.size} requests failed: ${failures.distinct.map(f => s"${f.getClass}: $f").mkString(", ")} ") {
          succeeds.length shouldBe totalMsgs
        }

      }

      enterBarrier("finished")
    }

  }
}

object ClusterAwareBackendLoadBalance2Spec {
  case class Reply(m: Any, replyTo: ActorRef)
  class DelayedEcho extends Actor with ActorLogging with MessageScheduler {
    override def receive: Receive = {
      case Reply(m, replyTo) => replyTo ! m
      case m => delayedMsg(10.milliseconds, Reply(m, sender))
    }
  }
}

