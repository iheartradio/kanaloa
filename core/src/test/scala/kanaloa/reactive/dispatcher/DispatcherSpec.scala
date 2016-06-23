package kanaloa.reactive.dispatcher

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import kanaloa.reactive.dispatcher.ApiProtocol.{ShutdownGracefully, ShutdownSuccessfully, WorkFailed, WorkRejected}
import kanaloa.reactive.dispatcher.metrics.{NoOpMetricsCollector, StatsDMetricsCollector}
import kanaloa.reactive.dispatcher.queue.ProcessingWorkerPoolSettings
import kanaloa.reactive.dispatcher.queue.TestUtils.MessageProcessed

import scala.concurrent.Future
import concurrent.duration._

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
        None,
        {
          case Success ⇒ Right(())
        }
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

    "stop in the middle of processing a list" in new ScopeWithActor {
      import akka.actor.ActorDSL._
      val echoSuccess = actor(new Act {
        become {
          case _ ⇒ sender ! Success
        }
      })

      val iterator = Stream.continually(1).iterator
      val pwp = system.actorOf(Props(PullingDispatcher(
        "test",
        iterator,
        Dispatcher.defaultDispatcherSettings().copy(workerPool = ProcessingWorkerPoolSettings(1), autoScaling = None),
        echoSuccess,
        metricsCollector = NoOpMetricsCollector,
        None,
        {
          case Success ⇒ Right(())
        }
      )))

      expectNoMsg(20.milliseconds)
      pwp ! ShutdownGracefully(Some(self))

      expectMsg(ShutdownSuccessfully)
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
    "receive WorkRejected messages if queue is at capacity" in new ScopeWithActor {
      val config = ConfigFactory.parseString("kanaloa.default-dispatcher.backPressure.maxBufferSize=0")
      val dispatcher = system.actorOf(PushingDispatcher.props(
        "test",
        (i: String) ⇒ Future.successful("A Result"),
        config
      )(ResultChecker.simple[MessageProcessed]))
      dispatcher ! "message"
      expectMsgType[WorkRejected]
    }
  }

  "readConfig" should {
    "use default settings when nothing is in config" in {
      val (settings, mc) = Dispatcher.readConfig("example", ConfigFactory.empty)
      settings.workRetry === 0
      settings.autoScaling shouldBe defined
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
      settings.autoScaling shouldBe defined
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
      settings.autoScaling shouldBe None
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
      settings.backPressure shouldBe None
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
      settings.circuitBreaker shouldBe None
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
            kanaloa.default-dispatcher {
              metrics {
                enabled  = on
                statsd {
                  host = "localhost"
                  eventSampleRate = 0.5
                }
              }
            }
          """

      val (_, mc) = Dispatcher.readConfig("example", ConfigFactory.parseString(cfgStr))
      mc shouldBe a[StatsDMetricsCollector]
      mc.asInstanceOf[StatsDMetricsCollector].eventSampleRate === 0.5
    }

    "turn off metrics collector when disabled at the dispatcher level" in {
      val cfgStr =
        """
            kanaloa {
              dispatchers {
                example {
                  metrics.enabled = off
                }
              }
              dispatchers {
                example2 { }
              }

              default-dispatcher {
                metrics {
                  enabled = on
                  statsd {
                    host = "localhost"
                    eventSampleRate = 0.5
                  }
                }
              }
            }
          """

      val strCfg: Config = ConfigFactory.parseString(cfgStr)
      val (_, mc) = Dispatcher.readConfig("example", strCfg)
      mc === NoOpMetricsCollector

      val (_, mc2) = Dispatcher.readConfig("example2", strCfg)
      mc2 shouldBe a[StatsDMetricsCollector]
    }

    "override collector settings at the dispatcher level" in {
      val cfgStr =
        """
            kanaloa {
              dispatchers {
                example {
                  metrics {
                    statsd {
                      host = "localhost"
                      eventSampleRate = 0.7
                    }
                  }
                }
              }
              default-dispatcher.metrics {
                enabled  = on
                statsd {
                  host = "localhost"
                  eventSampleRate = 0.5
                }
              }
            }
          """

      val strCfg: Config = ConfigFactory.parseString(cfgStr)
      val (_, mc) = Dispatcher.readConfig("example", strCfg)
      mc shouldBe a[StatsDMetricsCollector]
      mc.asInstanceOf[StatsDMetricsCollector].eventSampleRate === 0.7

    }

    "turn off metrics collector when disabled at the config level" in {
      val cfgStr =
        """
            kanaloa {
              default-dispatcher.metrics {
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
              default-dispatcher.metrics {
                enabled = on
                statsd {}
              }
            }
          """

      intercept[ConfigException] {
        Dispatcher.readConfig("example", ConfigFactory.parseString(cfgStr))
      }
    }
  }
}

class ScopeWithActor(implicit system: ActorSystem) extends TestKit(system) with ImplicitSender {
  case object Success

  val delegatee = TestProbe()

  val backend: Backend = delegatee.ref
}
