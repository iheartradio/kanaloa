package kanaloa.dispatcher

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActors, TestKit, TestProbe}
import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import kanaloa.dispatcher.ApiProtocol._
import kanaloa.dispatcher.metrics.{StatsDClient, Metric, MetricsCollector, StatsDReporter}
import kanaloa.dispatcher.queue.TestUtils.MessageProcessed
import kanaloa.dispatcher.queue._
import org.scalatest.OptionValues
import org.scalatest.concurrent.Eventually

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

class DispatcherSpec extends SpecWithActorSystem with OptionValues {
  implicit val noneStatsDClient: Option[StatsDClient] = None
  "pulling work dispatcher" should {

    "finish a simple list" in new ScopeWithActor {
      val iterator = List(1, 3, 5, 6).iterator
      val pwp = system.actorOf(Props(PullingDispatcher(
        "test",
        iterator,
        Dispatcher.defaultDispatcherSettings().copy(workerPool = ProcessingWorkerPoolSettings(1), autothrottle = None),
        backend,
        None,
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

    "shutdown in the middle of processing a list" in new ScopeWithActor {

      val iterator = Stream.continually(1).iterator
      val resultProbe = TestProbe()
      val dispatcher = system.actorOf(
        PullingDispatcher.props(
          "test",
          iterator,
          TestActors.echoActorProps,
          Some(resultProbe.ref)
        )(ResultChecker.complacent)
      )

      resultProbe.expectMsg(1)
      resultProbe.expectMsg(1)
      resultProbe.expectMsg(1)

      watch(dispatcher)

      dispatcher ! ShutdownGracefully(Some(self))

      expectMsg(ShutdownSuccessfully)
      expectTerminated(dispatcher)
    }

    "shutdown forcefully when timeout" in new ScopeWithActor {

      val iterator = Stream.continually(1).iterator
      val resultProbe = TestProbe()
      val backendProb = TestProbe()
      val dispatcher = system.actorOf(
        PullingDispatcher.props(
          "test",
          iterator,
          backendProb.ref,
          Some(resultProbe.ref),
          ConfigFactory.parseString("kanaloa.default-dispatcher.workerPool.startingPoolSize = 20")
        )(ResultChecker.complacent)
      )

      backendProb.expectMsg(1)
      backendProb.reply(1)
      resultProbe.expectMsg(1)
      backendProb.expectMsg(1)

      resultProbe.expectNoMsg(30.milliseconds)

      watch(dispatcher)

      dispatcher ! ShutdownGracefully(Some(self), 20.milliseconds)

      expectMsg(ShutdownForcefully)
      expectTerminated(dispatcher)
    }

    "shutdown before work starts" in new ScopeWithActor {
      //an iterator that doesn't return work immediately
      val iterator = new Iterator[Int] {
        def hasNext: Boolean = true
        def next(): Int = {
          Thread.sleep(500)
          1
        }
      }

      val dispatcher = system.actorOf(
        PullingDispatcher.props(
          "test",
          iterator,
          TestActors.echoActorProps
        )(ResultChecker.complacent)
      )

      dispatcher ! ShutdownGracefully(Some(self))
      expectMsg(ShutdownSuccessfully)
    }

    "shutdown before worker created" in new ScopeWithActor with Backends {
      val iterator = Stream.continually(1).iterator
      import scala.concurrent.ExecutionContext.Implicits.global

      val dispatcher = system.actorOf(
        PullingDispatcher.props(
          "test",
          iterator,
          promiseBackend(Promise[ActorRef])
        )(ResultChecker.complacent)
      )

      dispatcher ! ShutdownGracefully(Some(self))

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

    "send WorkRejected metric when message is rejected" in new ScopeWithActor with Eventually {
      val reporter = new MockReporter
      val dispatcher = system.actorOf(Props(new PushingDispatcher(
        "test",
        Dispatcher.defaultDispatcherSettings(),
        TestProbe().ref,
        Some(reporter),
        ResultChecker.complacent
      )))

      dispatcher ! Regulator.DroppingRate(1)
      dispatcher ! "message"
      eventually {
        reporter.reported should contain(Metric.WorkRejected)
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
            |  autothrottle.enabled = off
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

    //todo: move this to integration test once the integration re-org test PR is merged.
    "start to reject work when worker creation fails" in new ScopeWithActor with Eventually with Backends {
      import system.dispatcher
      val pd = system.actorOf(PushingDispatcher.props(
        "test",
        promiseBackend(Promise[ActorRef].failure(new Exception("failing backend"))),
        ConfigFactory.parseString(
          """
            |kanaloa.default-dispatcher {
            |  updateInterval = 10ms
            |  backPressure {
            |    durationOfBurstAllowed = 10ms
            |    referenceDelay = 2s
            |  }
            |}""".stripMargin
        )
      )(ResultChecker.complacent))

      eventually {
        (1 to 100).foreach(_ ⇒ pd ! "a work")
        expectMsgType[WorkRejected](20.milliseconds)
      }(PatienceConfig(5.seconds, 40.milliseconds))

    }

    //todo: move this to integration test once the integration re-org test PR is merged. 
    "be able to pick up work after worker finally becomes available" in new ScopeWithActor with Eventually with Backends {
      import scala.concurrent.ExecutionContext.Implicits.global
      val backendActorPromise = Promise[ActorRef]
      val dispatcher = system.actorOf(PushingDispatcher.props(
        "test",
        promiseBackend(backendActorPromise),
        ConfigFactory.parseString(
          """
            |kanaloa.default-dispatcher {
            |  updateInterval = 50ms
            |  backPressure {
            |    durationOfBurstAllowed = 30ms
            |    referenceDelay = 1s
            |  }
            |}""".stripMargin
        )
      )(ResultChecker.complacent))

      //reach the point that it starts to reject work
      eventually {
        (1 to 30).foreach(_ ⇒ dispatcher ! "a work")
        expectMsgType[WorkRejected](20.milliseconds)
      }(PatienceConfig(5.seconds, 40.milliseconds))

      backendActorPromise.complete(util.Success(system.actorOf(TestActors.echoActorProps)))
      //recovers after the worker become available
      eventually {
        dispatcher ! "a work"
        expectMsg(10.milliseconds, "a work")
      }(PatienceConfig(5.seconds, 40.milliseconds))

    }

  }

  "readConfig" should {
    "use default settings when nothing is in config" in {
      val (settings, reporter) = Dispatcher.readConfig("example", ConfigFactory.empty, None)
      settings.workRetry === 0
      settings.autothrottle shouldBe defined
      settings.workerPool.shutdownOnAllWorkerDeath shouldBe false
      reporter shouldBe empty
    }

    "use a specific default settings" in {
      val (settings, _) = Dispatcher.readConfig("example", ConfigFactory.empty, None, Some("default-pulling-dispatcher"))
      settings.workerPool.shutdownOnAllWorkerDeath shouldBe true
      settings.autothrottle shouldBe defined
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

      val (settings, _) = Dispatcher.readConfig("example", ConfigFactory.parseString(cfgStr), None)
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

      val (settings, _) = Dispatcher.readConfig("example", ConfigFactory.parseString(cfgStr), None)
      settings.workRetry === 29
      settings.autothrottle shouldBe defined
    }

    "turn off autothrottle if set to off" in {
      val cfgStr =
        """
            kanaloa {
              dispatchers {
                example {
                  autothrottle {
                    enabled = off
                  }
                }
              }
            }
          """
      val (settings, _) = Dispatcher.readConfig("example", ConfigFactory.parseString(cfgStr), None)
      settings.autothrottle shouldBe None
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
      val (settings, _) = Dispatcher.readConfig("example", ConfigFactory.parseString(cfgStr), None)
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

      val (settings, _) = Dispatcher.readConfig("example", ConfigFactory.parseString(cfgStr), None)
      settings.circuitBreaker.get.timeoutCountThreshold === 6
    }

    "parse statsD collector " in {
      val cfgStr =
        """
            kanaloa.default-dispatcher {
              metrics {
                enabled  = on
                statsD {
                  host = "localhost"
                  eventSampleRate = 0.5
                }
              }
            }
          """
      val statsDClient = StatsDClient(ConfigFactory.parseString("""kanaloa.statsD.host = 125.9.9.1 """))

      val (_, reporter) = Dispatcher.readConfig("example", ConfigFactory.parseString(cfgStr), statsDClient)
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
                  statsD {
                    host = "localhost"
                    eventSampleRate = 0.5
                  }
                }
              }
            }
          """
      val statsDClient = StatsDClient(ConfigFactory.parseString("""kanaloa.statsD.host = 125.9.9.1 """))
      statsDClient shouldNot be(empty)

      val strCfg: Config = ConfigFactory.parseString(cfgStr)
      val (_, reporter) = Dispatcher.readConfig("example", strCfg, statsDClient)
      reporter shouldBe empty

      val (_, reporter2) = Dispatcher.readConfig("example2", strCfg, statsDClient)
      reporter2.value shouldBe a[StatsDReporter]
    }

    "override collector settings at the dispatcher level" in {
      val cfgStr =
        """
            kanaloa {
              dispatchers {
                example {
                  metrics {
                    statsD {
                      host = "localhost"
                      eventSampleRate = 0.7
                    }
                  }
                }
              }
              default-dispatcher.metrics {
                enabled  = on
                statsD {
                  host = "localhost"
                  eventSampleRate = 0.5
                }
              }
            }
          """

      val statsDClient = StatsDClient(ConfigFactory.parseString("""kanaloa.statsD.host = 125.9.9.1 """))

      val strCfg: Config = ConfigFactory.parseString(cfgStr)
      val (_, reporter) = Dispatcher.readConfig("example", strCfg, statsDClient)
      reporter.value shouldBe a[StatsDReporter]
      reporter.get.asInstanceOf[StatsDReporter].eventSampleRate === 0.7

    }

    "turn off metrics collector when disabled at the config level" in {
      val cfgStr =
        """
            kanaloa {
              default-dispatcher.metrics {
                enabled = off
                statsD {
                  host = "localhost"
                  eventSampleRate = 0.5
                }
              }
            }
          """
      val statsDClient = StatsDClient(ConfigFactory.parseString("""kanaloa.statsD.host = 125.9.9.1 """))

      val (_, reporter) = Dispatcher.readConfig("example", ConfigFactory.parseString(cfgStr), statsDClient)
      reporter shouldBe empty

    }

    "throw exception when host is missing" in {
      val cfgStr =
        """
            kanaloa {
              statsD {
                port = 2323
              }
            }
          """

      intercept[ConfigException] {
        StatsDClient(ConfigFactory.parseString(cfgStr))
      }
    }
  }
}

class ScopeWithActor(implicit system: ActorSystem) extends TestKit(system) with ImplicitSender {
  case object Success

  val delegatee = TestProbe()

  val backend: Backend = delegatee.ref
}
