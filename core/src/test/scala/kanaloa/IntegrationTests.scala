package kanaloa

import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicLong

import akka.actor._
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import kanaloa.ApiProtocol.{WorkRejected, QueryStatus, ShutdownGracefully, ShutdownSuccessfully}
import kanaloa.Dispatcher.SubscribePerformanceMetrics
import kanaloa.IntegrationTests._
import kanaloa.queue.QueueSampler
import kanaloa.queue.WorkerPoolSampler.{Report, WorkerPoolSample}
import kanaloa.handler.{HandlerProvider, GeneralActorRefHandler}
import kanaloa.handler.GeneralActorRefHandler.ResultChecker
import kanaloa.metrics.StatsDClient
import kanaloa.queue.WorkerPoolManager.{RunningStatus, ShuttingDown}
import kanaloa.util.Java8TimeExtensions._
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.language.reflectiveCalls

trait IntegrationSpec extends WordSpecLike with Matchers {

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

      }
    """
  ))

  lazy val statsDHostO = sys.env.get("KANALOA_STRESS_STATSD_HOST") //hook with statsD if its available
  lazy val metricsConfig = statsDHostO.map { _ ⇒
    s"""
      metrics {
        enabled = on // turn it off if you don't have a statsD server and hostname set as an env var
        statsD {
          namespace = kanaloa-integration
          eventSampleRate = 0.25
        }
      }
    """
  }.getOrElse("")

  lazy implicit val statsDClient = statsDHostO.flatMap { host ⇒
    StatsDClient(ConfigFactory.parseString(s"kanaloa.statsD.host = $host"))
  }

  def afterAll(): Unit = system.terminate()

}

class PushingDispatcherIntegration extends IntegrationSpec {

  "can process through a large number of messages" in new TestScope {

    val backend = TestActorRef[SimpleBackend]

    val pd = system.actorOf(PushingDispatcher.props(
      "test-pushing",
      handlerWith(backend),
      ConfigFactory.parseString(
        s"""
          kanaloa.dispatchers.test-pushing {
            worker-pool {
              starting-pool-size = 8
            }
          }
        """
      )
    ))

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
      handlerWith(backend),
      ConfigFactory.parseString(
        s"""
          kanaloa.dispatchers.test-pushing {
            worker-pool {
              starting-pool-size = 8
            }
            autothrottle {
              enabled = off
            }
            back-pressure {
              enabled = off
            }
            circuit-breaker {
              enabled = off
            }
          }
        """
      )
    ))

    ignoreMsg { case Success ⇒ true }

    val messagesSent = sendLoadsOfMessage(pd, duration = 2.seconds, msgPerMilli = 1, verbose)

    shutdown(pd)

    backend.underlyingActor.count === messagesSent

  }
}

class PushingDispatcherAutoShutdownIntegration extends IntegrationSpec {

  "reject requests when backend died" in new TestScope {

    val backend = TestActorRef[SimpleBackend]

    val pd = system.actorOf(PushingDispatcher.props(
      "test-pushing",
      handlerWith(backend),
      ConfigFactory.parseString(
        s"""
          kanaloa.dispatchers.test-pushing {
          }
        """
      )
    ))

    ignoreMsg { case Success ⇒ true }

    watch(pd)

    backend ! PoisonPill

    Thread.sleep(60)

    pd ! 1

    expectMsgType[WorkRejected]

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
      handlerWith(backend),
      None,
      ConfigFactory.parseString(
        s"""
          kanaloa.dispatchers.test-pulling {
            worker-pool {
              starting-pool-size = 8
            }
          }
        """
      )
    ))

    watch(pd)

    expectTerminated(pd, shutdownTimeout)

    iterator.messageCount.get() === numOfMessages
  }

}

class PullingDispatcherSanityCheckIntegration extends IntegrationSpec {

  "can remain sane when all workers are constantly failing" in new TestScope with TestUtils.MockActors {
    val backend = system.actorOf(suicidal(1.milliseconds), "suicidal-backend")
    val iterator = Stream.continually("a").iterator

    val pd = system.actorOf(PullingDispatcher.props(
      "test-pulling",
      iterator,
      handlerWith(backend),
      None,
      ConfigFactory.parseString(
        s"""
          kanaloa.dispatchers.test-pulling {
            update-interval = 100ms
            worker-pool {
              starting-pool-size = 30
              min-pool-size = 30
              shutdown-on-all-worker-death = false
            }
            $metricsConfig
          }
        """
      )
    ), "pulling-dispatcher-22")

    watch(pd)

    val prob = TestProbe("probe-for-metrics")
    pd ! SubscribePerformanceMetrics(prob.ref)

    var samples = List[QueueSampler.QueueSample]() //collect 20 samples
    val r = prob.fishForMessage(20.seconds) {
      case s: QueueSampler.QueueSample ⇒
        samples = s :: samples
        samples.length > 20
      case p: QueueSampler.Report ⇒
        false
    }

    samples.forall(_.queueLength.value <= 30) shouldBe true

    samples.map(_.dequeued).sum shouldBe <=(30)

    pd ! ShutdownGracefully(timeout = 100.milliseconds)

    expectTerminated(pd, 200.milliseconds)

  }

}

object IntegrationTests {
  val shutdownTimeout = 60.seconds

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

  class SimpleBackend extends Actor {
    var count = 0
    def receive = {
      case msg ⇒
        count += 1
        sender ! Success
    }
  }

  val resultChecker: ResultChecker[Any, String] = {
    case Success ⇒ Right(Success)
    case _       ⇒ Left(Some("failure"))
  }

  def handlerWith(ref: ActorRef)(implicit system: ActorSystem): HandlerProvider[Any] = {
    import system.dispatcher
    HandlerProvider.actorRef("testBackend", ref, system)(resultChecker)
  }

  def iteratorOf(numberOfMessages: Int) = new Iterator[Int] {
    var messageCount = new AtomicLong()
    def hasNext: Boolean = messageCount.get() < numberOfMessages
    def next(): Int = {
      messageCount.getAndIncrement().toInt
    }
  }

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

    def getPoolSize(rd: Dispatcher[_]): Int = {
      rd.workerPools.head._2 ! QueryStatus() //note: this is assuming there is only one workerPool

      val status = expectMsgClass(classOf[RunningStatus])

      status.pool.size
    }

    def shutdown(rd: ActorRef): Unit = {
      rd ! ShutdownGracefully(Some(self), timeout = shutdownTimeout)

      expectMsg(shutdownTimeout, ShutdownSuccessfully)
    }
  }

}
