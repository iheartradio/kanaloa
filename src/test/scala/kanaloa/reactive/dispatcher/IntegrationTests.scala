package kanaloa.reactive.dispatcher

import akka.actor.{ ActorSystem, Props, ActorRef, Actor }
import akka.testkit.{ TestActorRef, TestProbe, ImplicitSender, TestKit }
import com.typesafe.config.ConfigFactory
import kanaloa.reactive.dispatcher.ApiProtocol.{ ShutdownSuccessfully, ShutdownGracefully }
import org.specs2.mutable.Specification
import org.specs2.specification.Scope
import scala.concurrent.Future
import scala.concurrent.duration._
import IntegrationTests._

trait IntegrationSpec extends Specification {
  sequential

  implicit lazy val system = ActorSystem("test", ConfigFactory.parseString(
    """
      |akka {
      |log-dead-letters = off
      |log-dead-letters-during-shutdown = off
      |scheduler {
      |  tick-duration = 1ms
      |  ticks-per-wheel = 2
      |}
      |}
    """.stripMargin
  ))
  def afterAll(): Unit = system.terminate()

}

class PushingDispatcherIntegrationSpec extends IntegrationSpec {

  "can process through a large number of messages" in new TestScope {

    val backend = TestActorRef[SimpleBackend]

    val pd = system.actorOf(PushingDispatcher.props(
      "test-pushing",
      Backend(backend),
      ConfigFactory.parseString(
        s"""
          |kanaloa.dispatchers.test-pushing {
          |  workerPool {
          |    startingPoolSize = 8
          |  }
          |}
        """.stripMargin
      )
    )(resultChecker))

    ignoreMsg { case Success ⇒ true }

    val messagesSent = sendLoadsOfMessage(pd, duration = 1.seconds, msgPerMilli = 3)

    expectNoMsg(200.milliseconds) //wait for all message to be processed

    pd ! ShutdownGracefully(Some(self))

    expectMsg(ShutdownSuccessfully)

    backend.underlyingActor.count === messagesSent

  }

}

class AutoScalingIntegrationSpec extends IntegrationSpec {

  "pushing dispatcher move to the optimal pool size" in new TestScope {

    val processTime = 2.milliseconds
    val optimalSize = 19
    val pd = system.actorOf(PushingDispatcher.props(
      "test-pushing",
      Backend(system.actorOf(backendProps(optimalSize, processTime))),
      ConfigFactory.parseString(
        """
          |kanaloa.dispatchers.test-pushing {
          |  workerPool {
          |    startingPoolSize = 5
          |    minPoolSize = 5
          |  }
          |  backPressure {
          |    maxBufferSize = 600000
          |    thresholdForExpectedWaitTime = 3000m
          |    maxHistoryLength = 3s
          |  }
          |  autoScaling {
          |    chanceOfScalingDownWhenFull = 0.1
          |    actionFrequency = 10m
          |    downsizeAfterUnderUtilization = 72h
          |  }
          |}
        """.stripMargin
      )
    )(resultChecker))

    ignoreMsg { case Success ⇒ true }

    val optimalSpeed = optimalSize / processTime.toMillis

    val sent = sendLoadsOfMessage(pd, duration = 2.minute, msgPerMilli = optimalSpeed.toInt)

    println("all messages sent, now shut down!")

    pd ! ShutdownGracefully(Some(self), timeout = 5.minutes)

    expectMsg(ShutdownSuccessfully)

  }

}

object IntegrationTests {

  case class Reply(to: ActorRef)

  case object Success

  class ConcurrencyLimitedBackend(optimalSize: Int, baseWait: FiniteDuration) extends Actor {
    var concurrent = 0

    import context.dispatcher

    def receive = {
      case Reply(to) ⇒
        concurrent -= 1
        to ! Success

      case msg ⇒
        concurrent += 1
        val overCap = Math.max(concurrent - optimalSize, 0)
        val wait = baseWait * (1 + overCap * overCap)
        context.system.scheduler.scheduleOnce(wait, self, Reply(sender))

    }
  }

  class SimpleBackend extends Actor {
    var count = 0
    def receive = {
      case msg ⇒
        count += 1
        sender ! Success
    }
  }

  val resultChecker: ResultChecker = {
    case Success ⇒ Right(Success)
    case m       ⇒ throw new Exception("Unexpected msg")
  }

  def backendProps(optimalSize: Int, baseWait: FiniteDuration = 1.milliseconds) =
    Props(new ConcurrencyLimitedBackend(optimalSize, baseWait))

  class TestScope(implicit system: ActorSystem) extends TestKit(system) with ImplicitSender with Scope {

    def sendLoadsOfMessage(target: ActorRef, duration: FiniteDuration, msgPerMilli: Int): Int = {

      val numberOfMessages = (duration.toMillis * msgPerMilli).toInt

      1.to(numberOfMessages).foreach { i ⇒
        target ! i
        if (i % msgPerMilli == 0) Thread.sleep(1)
      }

      numberOfMessages
    }
  }

}
