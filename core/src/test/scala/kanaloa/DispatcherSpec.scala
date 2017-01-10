package kanaloa

import akka.actor.{ActorRefFactory, ActorRef, ActorSystem, Props}
import akka.testkit._
import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import kanaloa.ApiProtocol._
import kanaloa.handler.GeneralActorRefHandler.ResultChecker
import kanaloa.handler.GeneralActorRefHandler.ResultChecker
import kanaloa.handler.HandlerProvider.HandlerChange
import kanaloa.handler._
import kanaloa.metrics.{StatsDClient, Metric, MetricsCollector, StatsDReporter}
import kanaloa.queue.Result
import kanaloa.queue.TestUtils.MessageProcessed
import kanaloa.queue._
import org.scalatest.OptionValues
import org.scalatest.concurrent.Eventually

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.util.Success
import TestHandlerProviders._

class DispatcherSpec extends SpecWithActorSystem with OptionValues {
  def fixedWorkerPoolSettings(size: Int): Dispatcher.Settings =
    Dispatcher.defaultDispatcherSettings().copy(workerPool = WorkerPoolSettings(startingPoolSize = size, minPoolSize = size, maxPoolSize = size))

  implicit val noneStatsDClient: Option[StatsDClient] = None

  "pulling work dispatcher" should {

    "finish a simple list" in new ScopeWithActor {
      val iterator = List(1, 3, 5, 6).iterator
      val pwp = system.actorOf(Props(PullingDispatcher(
        "test",
        iterator,
        Dispatcher.defaultDispatcherSettings().copy(workerPool = WorkerPoolSettings(1), autothrottle = None),
        testHandlerProvider(ResultChecker.expectType[SuccessResp.type]),
        None,
        None

      )))

      watch(pwp)
      delegatee.expectMsg(1)
      delegatee.reply(SuccessResp)
      delegatee.expectMsg(3)
      delegatee.reply(SuccessResp)
      delegatee.expectMsg(5)
      delegatee.reply(SuccessResp)
      delegatee.expectMsg(6)
      delegatee.reply(SuccessResp)

      expectTerminated(pwp)
    }

    "shutdown in the middle of processing a list" in new ScopeWithActor {

      val iterator = Stream.continually(1).iterator
      val resultProbe = TestProbe()
      val dispatcher = system.actorOf(
        PullingDispatcher.props(
          "test",
          iterator,
          testHandlerProvider(ResultChecker.complacent, TestActors.echoActorProps),
          Some(resultProbe.ref)
        )
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
          testHandlerProvider(ResultChecker.complacent, backendProb.ref),
          Some(resultProbe.ref),
          ConfigFactory.parseString("kanaloa.default-dispatcher.workerPool.starting-pool-size = 20")
        )
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
          testHandlerProvider(ResultChecker.complacent, TestActors.echoActorProps)
        )
      )

      dispatcher ! ShutdownGracefully(Some(self))
      expectMsg(ShutdownSuccessfully)
    }

    "shutdown before worker created" in new ScopeWithActor with MockServices {
      val iterator = Stream.continually(1).iterator
      import scala.concurrent.ExecutionContext.Implicits.global

      val dispatcher = system.actorOf(
        PullingDispatcher.props(
          "test",
          iterator,
          fromPromise(Promise[ActorRef], ResultChecker.complacent)
        )
      )

      dispatcher ! ShutdownGracefully(Some(self))

      expectMsg(ShutdownSuccessfully)
    }

  }

  "pushing work dispatcher" should {
    trait SimplePushingDispatchScope extends ScopeWithActor {
      import system.{dispatcher ⇒ ex}
      val dispatcher = system.actorOf(PushingDispatcher.props(
        name = "test",
        (i: String) ⇒ Future.successful(MessageProcessed(i))
      ))
    }

    "work happily with simpleBackend" in new SimplePushingDispatchScope {
      dispatcher ! "3"
      expectMsg(MessageProcessed("3"))
    }

    "let simple backend reject unrecognized message" in new SimplePushingDispatchScope {
      dispatcher ! 3
      expectMsgType[WorkFailed]
    }

    "create worker pools from new handler" in new ScopeWithActor {
      val handlerProvider = new MockHandlerProvider()

      val dispatcher = system.actorOf(
        Props(PushingDispatcher("test", fixedWorkerPoolSettings(2), handlerProvider, None))
      )
      val serviceOne = TestProbe()
      handlerProvider.createHandler(serviceOne)

      dispatcher ! "Moo"
      dispatcher ! "Moo"
      serviceOne.expectMsg("Moo")
      serviceOne.expectMsg("Moo")

      val serviceTwo = TestProbe()

      handlerProvider.createHandler(serviceTwo)
      dispatcher ! "Baa"
      dispatcher ! "Baa"
      serviceTwo.expectMsg("Baa")
      serviceTwo.expectMsg("Baa")

    }

    "remove worker pools once handler is removed" in new ScopeWithActor {
      val handlerProvider = new MockHandlerProvider()

      val dispatcher = system.actorOf(
        Props(PushingDispatcher("test", fixedWorkerPoolSettings(1), handlerProvider, None))
      )
      val serviceOne = TestProbe()
      val serviceTwo = TestProbe()

      handlerProvider.createHandler(serviceOne)
      val handlerTwo = handlerProvider.createHandler(serviceTwo)

      dispatcher ! "Moo"
      dispatcher ! "Moo"
      serviceOne.expectMsg("Moo")
      serviceTwo.expectMsg("Moo")

      serviceOne.reply(Result("cow"))
      serviceTwo.reply(Result("cow"))
      expectMsg("cow")
      expectMsg("cow")

      handlerProvider.terminateHandler(handlerTwo)
      expectNoMsg() //give some time for termination to take effect
      dispatcher ! "Baa"
      dispatcher ! "Baa"
      serviceOne.expectMsg("Baa")
      serviceTwo.expectNoMsg()

    }

    "does not allow handlers with duplicated names" in new ScopeWithActor with Eventually {
      val handlerProvider = new MockHandlerProvider()
      import system.{dispatcher ⇒ executionContext}
      val dispatcher = TestActorRef[PushingDispatcher[Any]](
        Props(PushingDispatcher("test", fixedWorkerPoolSettings(1), handlerProvider, None))
      )
      val functionStub = (r: Any) ⇒ Future.successful("blah")
      val handler1: Handler[Any] = new SimpleFunctionHandler(functionStub, "name1")

      handlerProvider.injectHandler(handler1)

      eventually {
        dispatcher.underlyingActor.workerPools.size should be(1)
      }

      val handler2 = new SimpleFunctionHandler(functionStub, "name2")

      handlerProvider.injectHandler(handler2)

      eventually {
        dispatcher.underlyingActor.workerPools.size should be(2)
      }

      val handlerDup: Handler[Any] = new SimpleFunctionHandler(functionStub, "name2")

      handlerProvider.injectHandler(handlerDup)

      expectNoMsg() //wait a bit for the potential check to happen

      dispatcher.underlyingActor.workerPools.size should be(2)

    }

    "let simple result check fail on unrecognized reply message" in new ScopeWithActor {
      import system.{dispatcher ⇒ ex}
      val service = system.actorOf(TestActors.echoActorProps)
      val dispatcher = system.actorOf(PushingDispatcher.props(
        name = "test",
        HandlerProvider.actorRef("test", service, system) {
          case i: Int ⇒ Right(i)
          case _      ⇒ Left(Some("unrecognized"))
        }
      ))

      dispatcher ! "3"
      expectMsgType[WorkFailed]
    }

    "shutdown before worker pool is created" in new ScopeWithActor {
      import system.{dispatcher ⇒ ex}
      val service = system.actorOf(TestActors.echoActorProps)
      val dispatcher = system.actorOf(PushingDispatcher.props(
        name = "test",
        TestHandlerProviders.fromPromise(Promise[ActorRef](), simpleResultChecker)
      ))

      dispatcher ! ShutdownGracefully(Some(self))
      expectMsg(ShutdownSuccessfully)
    }

    "receive WorkRejected messages if queue is at capacity" in new ScopeWithActor {
      val backendProb = TestProbe()
      val dispatcher = system.actorOf(PushingDispatcher.props(
        "test",
        HandlerProvider.actorRef("testService", backendProb.ref)(ResultChecker.complacent)
      ))

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
        HandlerProvider.actorRef("testService", TestProbe().ref)(ResultChecker.complacent),
        Some(reporter)
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
        HandlerProvider.actorRef("testService", backendProb.ref)(ResultChecker.complacent),

        ConfigFactory.parseString(
          """
                |kanaloa.default-dispatcher {
                |  update-interval = 300s
                |  circuit-breaker.enabled = off
                |  autothrottle.enabled = off
                |}""".stripMargin
        ) //make sure regulator doesn't interfere
      ))

      dispatcher ! Regulator.DroppingRate(0.5)

      val numOfWork = 500

      (1 to numOfWork).foreach(_ ⇒ dispatcher ! "message")

      val received = backendProb.receiveWhile(30.seconds, 500.milliseconds) {
        case "message" ⇒ backendProb.reply("1")
      }

      (received.length.toDouble / numOfWork.toDouble) shouldBe 0.5 +- 0.07
    }

    //todo: move this to integration test once the integration re-org test PR is merged.
    "start to reject work when worker creation fails" in new ScopeWithActor with Eventually with MockServices {
      import system.dispatcher
      val pd = system.actorOf(PushingDispatcher.props(
        "test",
        fromPromise(Promise[ActorRef].failure(new Exception("failing backend")), ResultChecker.complacent),
        ConfigFactory.parseString(
          """
                |kanaloa.default-dispatcher {
                |  update-interval = 10ms
                |  back-pressure {
                |    duration-of-burst-allowed = 10ms
                |    reference-delay = 2s
                |  }
                |}""".stripMargin
        )
      ))

      eventually {
        (1 to 100).foreach(_ ⇒ pd ! "a work")
        expectMsgType[WorkRejected](20.milliseconds)
      }(PatienceConfig(5.seconds, 40.milliseconds))

    }

    //todo: move this to integration test once the integration re-org test PR is merged. 
    "be able to pick up work after worker finally becomes available" in new ScopeWithActor with Eventually with MockServices {
      import scala.concurrent.ExecutionContext.Implicits.global
      val backendActorPromise = Promise[ActorRef]
      val dispatcher = system.actorOf(PushingDispatcher.props(
        "test",
        fromPromise(backendActorPromise, ResultChecker.complacent),
        ConfigFactory.parseString(
          """
                |kanaloa.default-dispatcher {
                |  update-interval = 50ms
                |  back-pressure {
                |    duration-of-burst-allowed = 30ms
                |    reference-delay = 1s
                |  }
                |}""".stripMargin
        )
      ))

      //reach the point that it starts to reject work
      eventually {
        (1 to 30).foreach(_ ⇒ dispatcher ! "a work")
        expectMsgType[WorkRejected](20.milliseconds)
      }(PatienceConfig(5.seconds, 40.milliseconds))

      backendActorPromise.complete(Success(system.actorOf(TestActors.echoActorProps)))
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
      reporter shouldBe empty
    }

    "use a specific default settings" in {
      val (settings, _) = Dispatcher.readConfig("example", ConfigFactory.empty, None, Some("default-pulling-dispatcher"))
      settings.autothrottle shouldBe defined
    }

    "use default-dispatcher settings when dispatcher name is missing in the dispatchers section" in {
      val cfgStr =
        """
              kanaloa {
                default-dispatcher {
                  work-retry = 27
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
                  work-retry = 29
                }
                dispatchers {
                  example {
                    work-timeout = 1m
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

    "turn off circuit-breaker if set to off" in {
      val cfgStr =
        """
              kanaloa {
                dispatchers {
                  example {
                    circuit-breaker {
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
                    circuit-breaker {
                      timeout-count-threshold = 0.5
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
  case object SuccessResp

  val delegatee = TestProbe()

  def testHandler[TResp, TError](resultChecker: Any ⇒ Either[Option[TError], TResp], routee: ActorRef = delegatee.ref): Handler[Any] =
    new GeneralActorRefHandler("routeeHandler", routee, system)(resultChecker)

  def testHandlerProvider[TResp, TError](resultChecker: ResultChecker[TError, TResp], routee: ActorRef = delegatee.ref): HandlerProvider[Any] =
    testHandlerProvider(testHandler(resultChecker, routee))

  def testHandlerProvider[TResp, TError](
    resultChecker: ResultChecker[TError, TResp], props: Props
  )(
    implicit
    arf: ActorRefFactory
  ): HandlerProvider[Any] =
    testHandlerProvider(testHandler(resultChecker, arf.actorOf(props)))

  def testHandlerProvider(handler: Handler[Any]): HandlerProvider[Any] =
    new HandlerProvider[Any] {
      val handlers = List(handler)

      override def subscribe(f: (HandlerChange) ⇒ Unit): Unit = ()
    }
}
