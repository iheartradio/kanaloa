package kanaloa

import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicLong

import akka.actor._
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import kanaloa.ApiProtocol.{QueryStatus, ShutdownGracefully, ShutdownSuccessfully}
import kanaloa.Dispatcher.SubscribePerformanceMetrics
import kanaloa.IntegrationTests._
import kanaloa.WorkerPoolSampler.{Report, WorkerPoolSample}
import kanaloa.handler.{HandlerProvider, GeneralActorRefHandler}
import kanaloa.handler.GeneralActorRefHandler.ResultChecker
import kanaloa.metrics.StatsDClient
import kanaloa.queue.WorkerPoolManager.{RunningStatus, ShuttingDown}
import kanaloa.util.Java8TimeExtensions._
import org.scalatest.{ShouldMatchers, WordSpecLike}

import scala.concurrent.duration._
import scala.language.reflectiveCalls

trait IntegrationSpec extends WordSpecLike with ShouldMatchers {

  val verbose = true

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

  "can remain sane when all workers are constantly failing" in new TestScope with MockServices {
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

class AutothrottleWithPushingIntegration extends IntegrationSpec {

  "pushing dispatcher move to the optimal pool size" in new TestScope {

    val processTime = 4.milliseconds //cannot be faster than this to keep up with the computation power.
    val optimalSize = 10

    def test: Int = {
      val backend = TestActorRef[ConcurrencyLimitedBackend](concurrencyLimitedBackendProps(optimalSize, processTime))
      val pd = TestActorRef[Dispatcher[Any]](PushingDispatcher.props(
        "test-pushing",
        handlerWith(backend),
        ConfigFactory.parseString(
          s"""
          kanaloa.dispatchers.test-pushing {
            update-interval = 100ms
            worker-pool {
              starting-pool-size = 3
              min-pool-size = 1
            }
            back-pressure {
              enabled = off
            }
            autothrottle {
              chance-of-scaling-down-when-full = 0.3
              resize-interval = 200ms
              weight-of-latency = 0.1
              exploration-ratio = 0.5
              max-explore-step-size = 1
            }
            $metricsConfig
          }
        """
        )
      ))
      ignoreMsg {
        case Success ⇒ true
      }

      val optimalSpeed = optimalSize.toDouble / processTime.toMillis

      val sent = sendLoadsOfMessage(pd, duration = 6.seconds, msgPerMilli = optimalSpeed * 0.9, verbose)

      val actualPoolSize = getPoolSize(pd.underlyingActor)

      shutdown(pd)

      backend.underlyingActor.count === sent

      actualPoolSize
    }

    val failRatio = performMultipleTests(test, optimalSize, 6)

    failRatio should be <= 0.34
  }
}

class AutothrottleWithPullingIntegration extends IntegrationSpec {

  "pulling dispatcher move to the optimal pool size" in new TestScope {

    val processTime = 4.milliseconds //cannot be faster than this to keep up with the computation power.
    val optimalSize = 10
    val optimalSpeed = optimalSize.toDouble / processTime.toMillis
    val duration = 20.seconds
    val msgPerMilli = optimalSpeed * 0.9
    val numberOfMessages = (duration.toMillis * msgPerMilli).toInt

    def dispatcherProps(backend: ActorRef) = PullingDispatcher.props(
      "test-pulling",
      iteratorOf(numberOfMessages),
      handlerWith(backend),
      None,
      ConfigFactory.parseString(
        s"""
          kanaloa.dispatchers.test-pulling {
            update-interval = 100ms
            worker-pool {
              starting-pool-size = 3
              min-pool-size = 1
            }

            autothrottle {
              chance-of-scaling-down-when-full = 0.1
              resize-interval = 100ms
              downsize-after-under-utilization = 72h
            }
            $metricsConfig
          }
        """
      )
    )

    val backendProps = concurrencyLimitedBackendProps(optimalSize, processTime, Some(numberOfMessages))

    def test: Int = {
      val backend = TestActorRef[ConcurrencyLimitedBackend](backendProps)
      val pd = TestActorRef[Dispatcher[Any]](dispatcherProps(backend))

      ignoreMsg { case Success ⇒ true }

      watch(backend)
      pd.underlyingActor.workerPools.head._2 ! QueryStatus()
      var lastPoolSize = 0
      import system.dispatcher

      fishForMessage(duration * 4) {
        case Terminated(`backend`) ⇒ true //it shutdowns itself after all messages are processed.
        case RunningStatus(pool) ⇒
          lastPoolSize = pool.size
          system.scheduler.scheduleOnce(duration / 200, pd.underlyingActor.workerPools.head._2, QueryStatus(Some(self)))
          false
        case ShuttingDown ⇒ false
        case m            ⇒ throw new Exception("unexpected, " + m)

      }

      lastPoolSize
    }

    val failRatio = performMultipleTests(test, optimalSize, 6)

    failRatio should be <= 0.34
  }
}

class AutothrottleDownSizeWithSparseTrafficIntegration extends IntegrationSpec {
  "downsize when the traffic is sparse" in new TestScope {
    val backend = TestActorRef[SimpleBackend]
    val pd = TestActorRef[Dispatcher[Any]](PushingDispatcher.props(
      "test-pushing",
      handlerWith(backend),
      ConfigFactory.parseString(
        """
          kanaloa.dispatchers.test-pushing {
            update-interval = 100ms
            worker-pool {
              starting-pool-size = 10
              min-pool-size = 2
            }
            autothrottle {
              resize-interval = 10ms
              downsize-after-under-utilization = 100ms
            }
          }
        """
      )
    ))

    pd ! "a msg"

    expectMsg(Success)

    expectNoMsg(200.milliseconds) //wait for the downsize to happen

    val actualPoolSize = getPoolSize(pd.underlyingActor)

    shutdown(pd)

    actualPoolSize === 2

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
        val wait = baseWait * (1.0 + Math.pow(overCap.toDouble, 1.7))

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

    def getPoolSize(rd: Dispatcher[_]): Int = {
      rd.workerPools.head._2 ! QueryStatus() //todo: this is assuming there is only one workerPool

      val status = expectMsgClass(classOf[RunningStatus])

      status.pool.size
    }

    def shutdown(rd: ActorRef): Unit = {
      rd ! ShutdownGracefully(Some(self), timeout = shutdownTimeout)

      expectMsg(shutdownTimeout, ShutdownSuccessfully)
    }

    def performMultipleTests(test: ⇒ Int, optimalSize: Int, numberOfTests: Int = 4) = {

      val targetSize = optimalSize + 1 //target pool size is a bit larger to fully utilize the optimal concurrency
      val sizeResults = (1 to numberOfTests).map(_ ⇒ test)

      //normalized distance from the optimal size
      def distanceFromOptimalSize(actualSize: Int): Double = {
        println("actual size:" + actualSize + "  optimal size: " + targetSize)
        Math.abs(actualSize - targetSize).toDouble / targetSize
      }

      val normalizedDistances = sizeResults.map(distanceFromOptimalSize)

      val failedTests = normalizedDistances.count(_ > 0.3)

      val failRatio = failedTests.toDouble / numberOfTests

      failRatio
    }
  }

}
