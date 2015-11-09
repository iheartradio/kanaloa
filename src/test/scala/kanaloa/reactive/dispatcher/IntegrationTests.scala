package kanaloa.reactive.dispatcher

import java.time.LocalDateTime

import akka.actor._
import akka.testkit.{ TestActorRef, TestProbe, ImplicitSender, TestKit }
import com.typesafe.config.ConfigFactory
import kanaloa.reactive.dispatcher.ApiProtocol.{ QueryStatus, ShutdownSuccessfully, ShutdownGracefully }
import kanaloa.reactive.dispatcher.queue.QueueProcessor.RunningStatus
import org.specs2.mutable.Specification
import org.specs2.specification.Scope
import scala.concurrent.{ Promise, Future }
import scala.concurrent.duration._
import IntegrationTests._
import kanaloa.util.Java8TimeExtensions._
import scala.util.Random

trait IntegrationSpec extends Specification {

  val verbose = false

  private val logLevel = if (verbose) "INFO" else "OFF"
  sequential

  implicit lazy val system = ActorSystem("test", ConfigFactory.parseString(
    s"""
      |akka {
      |  log-dead-letters = off
      |  log-dead-letters-during-shutdown = off
      |
      |  scheduler {
      |    tick-duration = 1ms
      |    ticks-per-wheel = 2
      |  }
      |  stdout-loglevel = ${logLevel}
      |
      |  loglevel = ${logLevel}
      |}
    """.stripMargin
  ))

  def afterAll(): Unit = system.terminate()

}

class PushingDispatcherIntegration extends IntegrationSpec {

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

    val messagesSent = sendLoadsOfMessage(pd, duration = 3.seconds, msgPerMilli = 3, verbose)

    expectNoMsg(200.milliseconds) //wait for all message to be processed

    shutdown(pd)

    backend.underlyingActor.count === messagesSent

  }

}

class AutoScalingGeneralIntegration extends IntegrationSpec {

  "pushing dispatcher move to the optimal pool size" in new TestScope {

    val processTime = 4.milliseconds //cannot be faster than this to keep up with the computation power.
    val optimalSize = 8

    val backend = TestActorRef[ConcurrencyLimitedBackend](concurrencyLimitedBackendProps(optimalSize, processTime))
    val pd = TestActorRef[Dispatcher](PushingDispatcher.props(
      "test-pushing",
      Backend(backend),
      ConfigFactory.parseString(
        """
          |kanaloa.dispatchers.test-pushing {
          |  workerPool {
          |    startingPoolSize = 3
          |    minPoolSize = 1
          |  }
          |  backPressure {
          |    maxBufferSize = 60000
          |    thresholdForExpectedWaitTime = 1h
          |    maxHistoryLength = 3s
          |  }
          |  autoScaling {
          |    chanceOfScalingDownWhenFull = 0.1
          |    actionFrequency = 100ms
          |    downsizeAfterUnderUtilization = 72h
          |  }
          |}
        """.stripMargin
      )
    )(resultChecker))

    ignoreMsg { case Success ⇒ true }

    val optimalSpeed = optimalSize.toDouble / processTime.toMillis

    val sent = sendLoadsOfMessage(pd, duration = 10.seconds, msgPerMilli = optimalSpeed * 0.4, verbose)

    val actualPoolSize = getPoolSize(pd.underlyingActor)

    expectNoMsg(1.second) //wait for all message to be processed

    shutdown(pd)

    actualPoolSize must be ~ (optimalSize +/- 2)
  }
}

class AutoScalingDownSizeWithSparseTrafficIntegration extends IntegrationSpec {
  "downsize when the traffic is sparse" in new TestScope {
    val backend = TestActorRef[SimpleBackend]
    val pd = TestActorRef[Dispatcher](PushingDispatcher.props(
      "test-pushing",
      Backend(backend),
      ConfigFactory.parseString(
        """
          |kanaloa.dispatchers.test-pushing {
          |  workerPool {
          |    startingPoolSize = 10
          |    minPoolSize = 2
          |  }
          |  autoScaling {
          |    actionFrequency = 10ms
          |    downsizeAfterUnderUtilization = 100ms
          |  }
          |}
        """.stripMargin
      )
    )(resultChecker))

    pd ! "a msg"

    expectMsg(Success)

    expectNoMsg(200.milliseconds) //wait for the downsize to happen

    val actualPoolSize = getPoolSize(pd.underlyingActor)

    shutdown(pd)

    actualPoolSize === 2

  }

}

object IntegrationTests {

  case class Reply(to: ActorRef, delay: Duration, scheduled: LocalDateTime = LocalDateTime.now)

  case object Success

  class Delay extends Actor {
    def receive = {
      case r @ Reply(to, delay, _) ⇒
        Thread.sleep(delay.toMillis)
        context.parent ! r
        context stop self
    }
  }

  class ConcurrencyLimitedBackend(optimalSize: Int, baseWait: FiniteDuration) extends Actor with ActorLogging {
    var concurrent = 0
    var count = 0
    val startAt = LocalDateTime.now

    def receive = {
      case Reply(to, _, scheduled) ⇒
        concurrent -= 1
        count += 1
        if (count % 1000 == 0) {
          log.info(s"Concurrency: $concurrent")
          log.info(s"Total processed: $count at speed ${count.toFloat / startAt.until(LocalDateTime.now).toMillis} messages/ms")
          log.info(s"Process Time this message: ${scheduled.until(LocalDateTime.now).toMillis} ms")
        }

        to ! Success

      case msg ⇒
        concurrent += 1
        val overCap = Math.max(concurrent - optimalSize, 0)
        val wait = baseWait * (1 + Math.pow(overCap, 1.7))

        context.actorOf(Props(classOf[Delay])) ! Reply(sender, wait)
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
  }

  def concurrencyLimitedBackendProps(optimalSize: Int, baseWait: FiniteDuration = 1.milliseconds) =
    Props(new ConcurrencyLimitedBackend(optimalSize, baseWait))

  class TestScope(implicit system: ActorSystem) extends TestKit(system) with ImplicitSender with Scope {

    def sendLoadsOfMessage(target: ActorRef, duration: FiniteDuration, msgPerMilli: Double, verbose: Boolean = false): Int = {

      val numberOfMessages = (duration.toMillis * msgPerMilli).toInt
      val start = LocalDateTime.now

      1.to(numberOfMessages).foreach { i ⇒
        target ! i
        if (i % (msgPerMilli * 50).toInt == 0) Thread.sleep(50)
        if (verbose && i % 1000 == 0) {
          println(s"Total message sent $i at speed ${i.toDouble / start.until(LocalDateTime.now).toMillis} messages/ms")
        }
      }

      numberOfMessages
    }

    def getPoolSize(rd: Dispatcher): Int = {
      rd.processor ! QueryStatus(Some(self))

      val status = expectMsgClass(classOf[RunningStatus])

      status.pool.size
    }

    def shutdown(rd: ActorRef): Unit = {
      rd ! ShutdownGracefully(Some(self), timeout = 10.seconds)

      expectMsg(10.seconds, ShutdownSuccessfully)
    }
  }

}
