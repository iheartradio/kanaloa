package kanaloa.reactive.dispatcher

import akka.actor.{ Props, ActorSystem }
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import com.typesafe.config.{ ConfigException, ConfigFactory }
import kanaloa.reactive.dispatcher.metrics.{ StatsDMetricsCollector, NoOpMetricsCollector }
import kanaloa.reactive.dispatcher.queue.ProcessingWorkerPoolSettings
import kanaloa.reactive.dispatcher.queue.TestUtils.Wrapper
import org.specs2.specification.Scope

class DispatcherSpec extends SpecWithActorSystem {
  "pulling work dispatcher" should {

    "finish a simple list" in new ScopeWithActor {
      val iterator = List(1, 3, 5, 6).iterator
      val pwp = system.actorOf(Props(PullingDispatcher(
        "test",
        iterator,
        Dispatcher.defaultDispatcherSettings.copy(workerPool = ProcessingWorkerPoolSettings(1), autoScaling = None),
        delegateeProps,
        metricsCollector = NoOpMetricsCollector,
        ({ case Success ⇒ Right(()) })
      )))

      delegatee.expectMsg(1)
      delegatee.reply(Success)
      delegatee.expectMsg(3)
      delegatee.reply(Success)
      delegatee.expectMsg(5)
      delegatee.reply(Success)
      delegatee.expectMsg(6)
      delegatee.reply(Success)
    }
  }

  "readConfig" should {
    "use default settings when missing" in {
      val (settings, mc) = Dispatcher.readConfig("example", ConfigFactory.empty)
      settings === Dispatcher.defaultDispatcherSettings
      mc === NoOpMetricsCollector
    }

    "parse settings that match the name" in {
      val cfgStr =
        """
          |kanaloa {
          |  dispatchers {
          |    example {
          |      circuitBreaker {
          |        errorRateThreshold = 0.5
          |      }
          |    }
          |  }
          |
          |}
        """.stripMargin

      val (settings, _) = Dispatcher.readConfig("example", ConfigFactory.parseString(cfgStr))
      settings.circuitBreaker.errorRateThreshold === 0.5
    }

    "parse statsD collector " in {
      val cfgStr =
        """
          |kanaloa {
          |  metrics {
          |    statsd {
          |      host = "localhost"
          |      eventSampleRate = 0.5
          |    }
          |  }
          |}
        """.stripMargin

      val (_, mc) = Dispatcher.readConfig("example", ConfigFactory.parseString(cfgStr))
      mc must beAnInstanceOf[StatsDMetricsCollector]
      mc.asInstanceOf[StatsDMetricsCollector].eventSampleRate === 0.5
    }

    "throw exception when host is missing" in {
      val cfgStr =
        """
          |kanaloa {
          |  metrics {
          |    statsd {
          |    }
          |  }
          |}
        """.stripMargin

      Dispatcher.readConfig("example", ConfigFactory.parseString(cfgStr)) must throwA[ConfigException]
    }
  }
}

class ScopeWithActor(implicit system: ActorSystem) extends TestKit(system) with ImplicitSender with Scope {
  case object Success

  val delegatee = TestProbe()

  val delegateeProps = Wrapper.props(delegatee.ref)
}
