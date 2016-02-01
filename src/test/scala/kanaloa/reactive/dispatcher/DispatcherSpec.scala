package kanaloa.reactive.dispatcher

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import kanaloa.reactive.dispatcher.ApiProtocol.WorkFailed
import kanaloa.reactive.dispatcher.metrics.{NoOpMetricsCollector, StatsDMetricsCollector}
import kanaloa.reactive.dispatcher.queue.ProcessingWorkerPoolSettings
import kanaloa.reactive.dispatcher.queue.TestUtils.MessageProcessed
import org.specs2.specification.Scope

import scala.concurrent.Future

class DispatcherSpec extends SpecWithActorSystem {
  "pulling work dispatcher" should {

    "finish a simple list" in new ScopeWithActor {
      val iterator = List(1, 3, 5, 6).iterator
      val pwp = system.actorOf(Props(PullingDispatcher(
        "test",
        iterator,
        Dispatcher.defaultDispatcherSettings().copy(workerPool = ProcessingWorkerPoolSettings(1), autoScaling = None),
        backend,
        metricsCollector = NoOpMetricsCollector,
        ({
          case Success ⇒ Right(())
        })
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

  "pushing work dispatcher" should {
    trait SimplePushingDispatchScope extends ScopeWithActor {
      val dispatcher = system.actorOf(PushingDispatcher.props(
        name = "test",
        (i: String) ⇒ Future.successful(MessageProcessed(i))
      )(ResultChecker.simple[MessageProcessed]))
    }

    "work happily with simpleBackend" in new SimplePushingDispatchScope {
      dispatcher ! "3"
      expectMsg(MessageProcessed("3"))
    }

    "let simple backend reject unrecognized message" in new SimplePushingDispatchScope {
      dispatcher ! 3
      expectMsgType[WorkFailed]
    }

    "let simple result check fail on unrecognized reply message" in new ScopeWithActor {
      val dispatcher = system.actorOf(PushingDispatcher.props(
        name = "test",
        (i: String) ⇒ Future.successful("A Result")
      )(ResultChecker.simple[MessageProcessed]))

      dispatcher ! "3"
      expectMsgType[WorkFailed]
    }
  }

  "readConfig" should {
    "use default settings when nothing is in config" in {
      val (settings, mc) = Dispatcher.readConfig("example", ConfigFactory.empty)
      settings.workRetry === 0
      settings.autoScaling must beSome
      mc === NoOpMetricsCollector
    }

    "use default-dispatcher settings when dispatcher name is missing in the dispatchers section" in {
      val cfgStr =
        """
          kanaloa {
            default-dispatcher {
              workRetry = 27
            }
            dispatchers {

            }
          }
        """

      val (settings, _) = Dispatcher.readConfig("example", ConfigFactory.parseString(cfgStr))
      settings.workRetry === 27
    }

    "fall back to default-dispatcher settings when a field is missing in the dispatcher section" in {
      val cfgStr =
        """
          kanaloa {
            default-dispatcher {
              workRetry = 29
            }
            dispatchers {
              example {
                workTimeout = 1m
              }
            }

          }
        """

      val (settings, _) = Dispatcher.readConfig("example", ConfigFactory.parseString(cfgStr))
      settings.workRetry === 29
      settings.autoScaling must beSome
    }

    "turn off autoScaling if set to off" in {
      val cfgStr =
        """
          kanaloa {
            dispatchers {
              example {
                autoScaling {
                  enabled = off
                }
              }
            }
          }
        """
      val (settings, _) = Dispatcher.readConfig("example", ConfigFactory.parseString(cfgStr))
      settings.autoScaling must beNone
    }

    "turn off backPressure if set to off" in {
      val cfgStr =
        """
          kanaloa {
            dispatchers {
              example {
                backPressure {
                  enabled = off
                }
              }
            }
          }
        """
      val (settings, _) = Dispatcher.readConfig("example", ConfigFactory.parseString(cfgStr))
      settings.backPressure must beNone
    }

    "turn off circuitBreaker if set to off" in {
      val cfgStr =
        """
          kanaloa {
            dispatchers {
              example {
                circuitBreaker {
                  enabled = off
                }
              }
            }
          }
        """
      val (settings, _) = Dispatcher.readConfig("example", ConfigFactory.parseString(cfgStr))
      settings.circuitBreaker must beNone
    }

    "parse settings that match the name" in {
      val cfgStr =
        """
          kanaloa {
            dispatchers {
              example {
                circuitBreaker {
                  errorRateThreshold = 0.5
                }
              }
            }
          }
        """

      val (settings, _) = Dispatcher.readConfig("example", ConfigFactory.parseString(cfgStr))
      settings.circuitBreaker.get.errorRateThreshold === 0.5
    }

    "parse statsD collector " in {
      val cfgStr =
        """
          kanaloa {
            metrics {
              statsd {
                host = "localhost"
                eventSampleRate = 0.5
              }
            }
          }
        """

      val (_, mc) = Dispatcher.readConfig("example", ConfigFactory.parseString(cfgStr))
      mc must beAnInstanceOf[StatsDMetricsCollector]
      mc.asInstanceOf[StatsDMetricsCollector].eventSampleRate === 0.5
    }

    "turn off metrics collector when disabled at the dispatcher level" in {
      val cfgStr =
        """
          kanaloa {
            dispatchers {
              example {
                metrics = off
              }
            }
            dispatchers {
              example2 { }
            }
            metrics {
              statsd {
                host = "localhost"
                eventSampleRate = 0.5
              }
            }
          }
        """

      val strCfg: Config = ConfigFactory.parseString(cfgStr)
      val (_, mc) = Dispatcher.readConfig("example", strCfg)
      mc === NoOpMetricsCollector

      val (_, mc2) = Dispatcher.readConfig("example2", strCfg)
      mc2 must beAnInstanceOf[StatsDMetricsCollector]
    }

    "turn off metrics collector when disabled at the config level" in {
      val cfgStr =
        """
          kanaloa {
            metrics {
              enabled = off
              statsd {
                host = "localhost"
                eventSampleRate = 0.5
              }
            }
          }
        """

      val (_, mc) = Dispatcher.readConfig("example", ConfigFactory.parseString(cfgStr))
      mc === NoOpMetricsCollector

    }

    "throw exception when host is missing" in {
      val cfgStr =
        """
          kanaloa {
            metrics {
              statsd {}
            }
          }
        """

      Dispatcher.readConfig("example", ConfigFactory.parseString(cfgStr)) must throwA[ConfigException]
    }
  }
}

class ScopeWithActor(implicit system: ActorSystem) extends TestKit(system) with ImplicitSender with Scope {
  case object Success

  val delegatee = TestProbe()

  val backend: Backend = delegatee.ref
}
