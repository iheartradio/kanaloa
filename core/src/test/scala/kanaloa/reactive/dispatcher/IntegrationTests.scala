package kanaloa.reactive.dispatcher

import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicLong

import akka.actor._
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import com.typesafe.config.ConfigFactory
import kanaloa.reactive.dispatcher.ApiProtocol.{QueryStatus, ShutdownGracefully, ShutdownSuccessfully}
import kanaloa.reactive.dispatcher.queue.QueueProcessor.{RunningStatus, ShuttingDown}

import scala.concurrent.duration._
import IntegrationTests._
import kanaloa.util.Java8TimeExtensions._
import org.scalatest.{ShouldMatchers, WordSpecLike}

import scala.language.reflectiveCalls

trait IntegrationSpec extends WordSpecLike with ShouldMatchers {

  val verbose = false

  private lazy val logLevel = if (verbose) "INFO" else "OFF"

  implicit lazy val system = ActorSystem("test", ConfigFactory.parseString(
    s"""
      akka {
        log-dead-letters = off
        log-dead-letters-during-shutdown = off

        scheduler {
          tick-duration = 1ms
          ticks-per-wheel = 2
        }
        stdout-loglevel = $logLevel

        loglevel = $logLevel
      }

      kanaloa.default-dispatcher {
        dispatchHistory {
          maxHistoryLength = 200ms
          historySampleRate = 20ms
        }
      }
    """
  ))

  def afterAll(): Unit = system.terminate()

}

class PushingDispatcherIntegration extends IntegrationSpec {

  "can process through a large number of messages" in new TestScope {

    val backend = TestActorRef[SimpleBackend]

    val pd = system.actorOf(PushingDispatcher.props(
      "test-pushing",
      backend,
      ConfigFactory.parseString(
        s"""
          kanaloa.dispatchers.test-pushing {
            workerPool {
              startingPoolSize = 8
            }
          }
        """
      )
    )(resultChecker))

    ignoreMsg { case Success ⇒ true }

    val messagesSent = sendLoadsOfMessage(pd, duration = 3.seconds, msgPerMilli = 1, verbose)

    shutdown(pd)

    backend.underlyingActor.count === messagesSent

  }

}

class MinimalPushingDispatcherIntegration extends IntegrationSpec {

  "can process through a large number of messages" in new TestScope {

    val backend = TestActorRef[SimpleBackend]

    val pd = system.actorOf(PushingDispatcher.props(
      "test-pushing",
      backend,
      ConfigFactory.parseString(
        s"""
          kanaloa.dispatchers.test-pushing {
            workerPool {
              startingPoolSize = 8
            }
            autoScaling {
              enabled = off
            }
            backPressure {
              enabled = off
            }
            circuitBreaker {
              enabled = off
            }
          }
        """
      )
    )(resultChecker))

    ignoreMsg { case Success ⇒ true }

    val messagesSent = sendLoadsOfMessage(pd, duration = 2.seconds, msgPerMilli = 1, verbose)

    shutdown(pd)

    backend.underlyingActor.count === messagesSent

  }

}

class PullingDispatcherIntegration extends IntegrationSpec {

  "can process through a large number of messages" in new TestScope {
    val numOfMessages = 1000
    val backend = TestActorRef[SimpleBackend]
    val iterator = iteratorOf(numOfMessages)
    val pd = system.actorOf(PullingDispatcher.props(
      "test-pulling",
      iterator,
      backend,
      None,
      ConfigFactory.parseString(
        s"""
          kanaloa.dispatchers.test-pulling {
            workerPool {
              startingPoolSize = 8
            }
          }
        """
      )
    )(resultChecker))

    watch(pd)

    expectTerminated(pd, shutdownTimeout)

    iterator.messageCount.get() === numOfMessages
  }

}

class AutoScalingWithPushingIntegration extends IntegrationSpec {

  "pushing dispatcher move to the optimal pool size" in new TestScope {

    val processTime = 4.milliseconds //cannot be faster than this to keep up with the computation power.
    val optimalSize = 8

    def test: Int = {
      val backend = TestActorRef[ConcurrencyLimitedBackend](concurrencyLimitedBackendProps(optimalSize, processTime))
      val pd = TestActorRef[Dispatcher](PushingDispatcher.props(
        "test-pushing",
        backend,
        ConfigFactory.parseString(
          """
          kanaloa.dispatchers.test-pushing {
            workerPool {
              startingPoolSize = 3
              minPoolSize = 1
            }
            backPressure {
              maxBufferSize = 60000
              thresholdForExpectedWaitTime = 1h
              maxHistoryLength = 3s
            }
            autoScaling {
              chanceOfScalingDownWhenFull = 0.1
              actionFrequency = 100ms
              downsizeAfterUnderUtilization = 72h
            }
          }
        """
        )
      )(resultChecker))
      ignoreMsg {
        case Success ⇒ true
      }

      val optimalSpeed = optimalSize.toDouble / processTime.toMillis

      val sent = sendLoadsOfMessage(pd, duration = 5.seconds, msgPerMilli = optimalSpeed * 0.4, verbose)

      val actualPoolSize = getPoolSize(pd.underlyingActor)

      shutdown(pd)

      backend.underlyingActor.count === sent

      actualPoolSize
    }

    performMultipleTests(test, optimalSize)
  }
}

class AutoScalingWithPullingIntegration extends IntegrationSpec {

  "pulling dispatcher move to the optimal pool size" in new TestScope {

    val processTime = 4.milliseconds //cannot be faster than this to keep up with the computation power.
    val optimalSize = 8
    val optimalSpeed = optimalSize.toDouble / processTime.toMillis
    val duration = 5.seconds
    val msgPerMilli = optimalSpeed * 0.4
    val numberOfMessages = (duration.toMillis * msgPerMilli).toInt

    def dispatcherProps(backend: ActorRef) = PullingDispatcher.props(
      "test-pulling",
      iteratorOf(numberOfMessages),
      backend,
      None,
      ConfigFactory.parseString(
        """
          kanaloa.dispatchers.test-pulling {
            workerPool {
              startingPoolSize = 3
              minPoolSize = 1
            }
            backPressure {
              maxBufferSize = 60000
              thresholdForExpectedWaitTime = 1h
              maxHistoryLength = 3s
            }
            autoScaling {
              chanceOfScalingDownWhenFull = 0.1
              actionFrequency = 100ms
              downsizeAfterUnderUtilization = 72h
            }
          }
        """
      )
    )(resultChecker)

    val backendProps = concurrencyLimitedBackendProps(optimalSize, processTime, Some(numberOfMessages))

    def test: Int = {
      val backend = TestActorRef[ConcurrencyLimitedBackend](backendProps)
      val pd = TestActorRef[Dispatcher](dispatcherProps(backend))

      ignoreMsg { case Success ⇒ true }

      watch(backend)
      pd.underlyingActor.processor ! QueryStatus()
      var lastPoolSize = 0
      import system.dispatcher

      fishForMessage(duration * 2) {
        case Terminated(`backend`) ⇒ true //it shutdowns itself after all messages are processed.
        case RunningStatus(pool) ⇒
          lastPoolSize = pool.size
          system.scheduler.scheduleOnce(duration / 200, pd.underlyingActor.processor, QueryStatus(Some(self)))
          false
        case ShuttingDown ⇒ false
        case m            ⇒ throw new Exception("unexpected, " + m)

      }

      lastPoolSize
    }

    val failRatio = performMultipleTests(test, optimalSize, 6)

    failRatio should be <= 0.5
  }
}

class AutoScalingDownSizeWithSparseTrafficIntegration extends IntegrationSpec {
  "downsize when the traffic is sparse" in new TestScope {
    val backend = TestActorRef[SimpleBackend]
    val pd = TestActorRef[Dispatcher](PushingDispatcher.props(
      "test-pushing",
      backend,
      ConfigFactory.parseString(
        """
          kanaloa.dispatchers.test-pushing {
            workerPool {
              startingPoolSize = 10
              minPoolSize = 2
            }
            autoScaling {
              actionFrequency = 10ms
              downsizeAfterUnderUtilization = 100ms
            }
          }
        """
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
  val shutdownTimeout = 30.seconds

  case class Reply(to: ActorRef, delay: Duration, scheduled: LocalDateTime = LocalDateTime.now)

  case object Success
  case class SuccessOf(msg: Int)

  class Delay extends Actor {
    def receive = {
      case r @ Reply(to, delay, _) ⇒
        Thread.sleep(delay.toMillis)
        context.parent ! r
        context stop self
    }
  }

  class ConcurrencyLimitedBackend(optimalSize: Int, baseWait: FiniteDuration, totalMessagesCap: Option[Int] = None) extends Actor with ActorLogging {
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
        totalMessagesCap.filter(count >= _).foreach { _ ⇒
          context stop self
        }

      case msg ⇒
        concurrent += 1
        val overCap = Math.max(concurrent - optimalSize, 0)
        val wait = baseWait * (1.0 + Math.pow(overCap, 1.7))

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

  def iteratorOf(numberOfMessages: Int) = new Iterator[Int] {
    var messageCount = new AtomicLong()
    def hasNext: Boolean = messageCount.get() < numberOfMessages
    def next(): Int = {
      messageCount.getAndIncrement().toInt
    }
  }

  def concurrencyLimitedBackendProps(optimalSize: Int, baseWait: FiniteDuration = 1.milliseconds, totalMessagesCap: Option[Int] = None) =
    Props(new ConcurrencyLimitedBackend(optimalSize, baseWait, totalMessagesCap))

  class TestScope(implicit system: ActorSystem) extends TestKit(system) with ImplicitSender {

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
      rd.processor ! QueryStatus()

      val status = expectMsgClass(classOf[RunningStatus])

      status.pool.size
    }

    def shutdown(rd: ActorRef): Unit = {
      rd ! ShutdownGracefully(Some(self), timeout = shutdownTimeout)

      expectMsg(shutdownTimeout, ShutdownSuccessfully)
    }

    def performMultipleTests(test: ⇒ Int, targetSize: Int, numberOfTests: Int = 4) = {
      val sizeResults = (1 to numberOfTests).map(_ ⇒ test)

      //normalized distance from the optimal size
      def distanceFromOptimalSize(actualSize: Int): Double = {
        Math.abs(actualSize - targetSize).toDouble / targetSize
      }

      val normalizedDistances = sizeResults.map(distanceFromOptimalSize)

      val failedTests = normalizedDistances.count(_ > 0.3)

      val failRatio = failedTests.toDouble / numberOfTests

      println("Results are  " + normalizedDistances.mkString(","))

      failRatio
    }
  }

}
