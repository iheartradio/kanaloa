package kanaloa.reactive.dispatcher

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import kanaloa.reactive.dispatcher.ApiProtocol.{ShutdownGracefully, ShutdownSuccessfully, WorkFailed, WorkRejected}
import kanaloa.reactive.dispatcher.PerformanceSampler.Subscribe
import kanaloa.reactive.dispatcher.metrics.{Metric, MetricsCollector, StatsDReporter}
import kanaloa.reactive.dispatcher.queue.ProcessingWorkerPoolSettings
import kanaloa.reactive.dispatcher.queue.TestUtils.MessageProcessed
import org.scalatest.OptionValues

import scala.concurrent.Future
import concurrent.duration._

class DispatcherSpec extends SpecWithActorSystem with OptionValues {
  "pulling work dispatcher" should {

    "finish a simple list" in new ScopeWithActor {
      val iterator = List(1, 3, 5, 6).iterator
      val pwp = system.actorOf(Props(PullingDispatcher(
        "test",
        iterator,
        Dispatcher.defaultDispatcherSettings().copy(workerPool = ProcessingWorkerPoolSettings(1), autoScaling = None),
        backend,
        metricsCollector = MetricsCollector(None),
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
        metricsCollector = MetricsCollector(None),
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
      )(ResultChecker.expectType[MessageProcessed]))
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
      )(ResultChecker.expectType[MessageProcessed]))

      dispatcher ! "3"
      expectMsgType[WorkFailed]
    }

    "receive WorkRejected messages if queue is at capacity" in new ScopeWithActor {
      val backendProb = TestProbe()
      val dispatcher = system.actorOf(PushingDispatcher.props(
        "test",
        backendProb.ref
      )(ResultChecker.complacent))

      dispatcher ! Regulator.DroppingRate(1)
      dispatcher ! "message"
      backendProb.expectNoMsg(40.milliseconds)
      expectMsgType[WorkRejected]
    }

    "send WorkRejected metric when message is rejected" in new ScopeWithActor {
      val metricCollector = TestProbe()
      val dispatcher = system.actorOf(Props(new PushingDispatcher(
        "test",
        Dispatcher.defaultDispatcherSettings(),
        TestProbe().ref,
        metricCollector.ref,
        ResultChecker.complacent
      )))

      dispatcher ! Regulator.DroppingRate(1)
      dispatcher ! "message"
      metricCollector.fishForMessage(30.milliseconds) {
        case Metric.WorkRejected ⇒ true
        case _                   ⇒ false
      }
    }

    "reject work according to drop rate" in new ScopeWithActor {
      val backendProb = TestProbe()
      val dispatcher = system.actorOf(PushingDispatcher.props(
        "test",
        backendProb.ref,
        ConfigFactory.parseString(
          """
            |kanaloa.default-dispatcher {
            |  updateInterval = 300s
            |  circuitBreaker.enabled = off
            |  autoScaling.enabled = off
            |}""".stripMargin
        ) //make sure regulator doesn't interfere
      )(ResultChecker.complacent))

      dispatcher ! Regulator.DroppingRate(0.5)

      val numOfWork = 500

      (1 to numOfWork).foreach(_ ⇒ dispatcher ! "message")

      val received = backendProb.receiveWhile(30.seconds, 500.milliseconds) {
        case "message" ⇒ backendProb.reply("1")
      }

      (received.length.toDouble / numOfWork.toDouble) shouldBe 0.5 +- 0.07
    }
  }

  "readConfig" should {
    "use default settings when nothing is in config" in {
      val (settings, reporter) = Dispatcher.readConfig("example", ConfigFactory.empty)
      settings.workRetry === 0
      settings.autoScaling shouldBe defined
      reporter shouldBe empty
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
                    timeoutCountThreshold = 0.5
                  }
                }
              }
            }
          """

      val (settings, _) = Dispatcher.readConfig("example", ConfigFactory.parseString(cfgStr))
      settings.circuitBreaker.get.timeoutCountThreshold === 6
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

      val (_, reporter) = Dispatcher.readConfig("example", ConfigFactory.parseString(cfgStr))
      reporter.value shouldBe a[StatsDReporter]
      reporter.get.asInstanceOf[StatsDReporter].eventSampleRate === 0.5
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
      val (_, reporter) = Dispatcher.readConfig("example", strCfg)
      reporter shouldBe empty

      val (_, reporter2) = Dispatcher.readConfig("example2", strCfg)
      reporter2.value shouldBe a[StatsDReporter]
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
      val (_, reporter) = Dispatcher.readConfig("example", strCfg)
      reporter.value shouldBe a[StatsDReporter]
      reporter.get.asInstanceOf[StatsDReporter].eventSampleRate === 0.7

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

      val (_, reporter) = Dispatcher.readConfig("example", ConfigFactory.parseString(cfgStr))
      reporter shouldBe empty

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
