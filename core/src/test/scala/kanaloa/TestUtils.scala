package kanaloa

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.ActorDSL._
import akka.actor._
import akka.testkit.TestProbe
import kanaloa.IntegrationTests.Success
import kanaloa.TestUtils.MockActors.SuicidalActor
import kanaloa.handler.{AgentHandlerProvider, GeneralActorRefHandler, HandlerProvider, Handler}
import kanaloa.queue.QueueTestUtils.DelegateeMessage
import kanaloa.queue.Sampler.SamplerSettings
import kanaloa.metrics.{Metric, Reporter}
import kanaloa.queue._
import kanaloa.queue.WorkerPoolManager.{CircuitBreakerFactory, AutothrottlerFactory, WorkerFactory, WorkerPoolSamplerFactory}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Promise}

object TestUtils {

  class Factories(implicit system: ActorSystem) {
    def silent(implicit ac: ActorRefFactory): ActorRef = actor {
      new Act {
        become {
          case _ ⇒
        }
      }
    }
    def fixedPoolSetting(size: Int) =
      WorkerPoolSettings(startingPoolSize = size, minPoolSize = size, maxPoolSize = size)

    def queueProps[T](
      iterator:         Iterator[T],
      metricsCollector: ActorRef         = TestProbe().ref,
      workSettings:     WorkSettings     = WorkSettings(),
      sendResultsTo:    Option[ActorRef] = None
    ): Props =
      Queue.ofIterator(iterator, metricsCollector, workSettings, sendResultsTo)

    def workerPoolSamplerFactory(
      queueSampler: ActorRef        = TestProbe().ref,
      settings:     SamplerSettings = SamplerSettings()
    ): WorkerPoolSamplerFactory = WorkerPoolSamplerFactory(queueSampler, settings)

    def workerPoolSampler(
      factory:   WorkerPoolSamplerFactory = workerPoolSamplerFactory(),
      reporter:  Option[Reporter]         = None,
      forwardTo: Option[ActorRef]         = None
    ): ActorRef = factory(reporter, forwardTo)

    def workerPoolSamplerFactory(
      returnRef: ActorRef
    ): WorkerPoolSamplerFactory = new WorkerPoolSamplerFactory {
      def apply(reporter: Option[Reporter], forwardTo: Option[ActorRef])(implicit ac: ActorRefFactory): ActorRef = returnRef
    }

    def workerPoolManagerProps[T](
      queue:                 ActorRef,
      handler:               Handler[T],
      settings:              WorkerPoolSettings            = WorkerPoolSettings(),
      workerFactory:         WorkerFactory                 = WorkerFactory.default,
      wps:                   WorkerPoolSamplerFactory      = workerPoolSamplerFactory(),
      autothrottlerFactory:  Option[AutothrottlerFactory]  = None,
      circuitBreakerFactory: Option[CircuitBreakerFactory] = None,
      reporter:              Option[Reporter]              = None
    ): Props =
      WorkerPoolManager.default(
        queue, handler, settings, workerFactory, wps, autothrottlerFactory, circuitBreakerFactory, reporter
      )
  }

  object HandlerProviders {
    def fromPromise[TError, TResp](
      promise:       Promise[ActorRef],
      resultChecker: Any ⇒ Either[Option[TError], TResp]
    )(
      implicit
      ex: ExecutionContext, system: ActorSystem
    ): HandlerProvider[Any] = HandlerProvider.actorRef("testPromise", promise.future, system)(resultChecker)

    val simpleResultChecker: Any ⇒ Either[Option[String], Any] = (v1: Any) ⇒ {
      v1 match {
        case Result(v) ⇒ Right(v)
        case Fail(v)   ⇒ Left(Option(v))
      }
    }

    def nameFromActorRef(actorRef: ActorRef): String =
      actorRef.path.toString.replace('/', '-')

    def simpleHandler(testProbe: TestProbe)(implicit system: ActorSystem): Handler[Any] =
      simpleHandler(testProbe.ref)

    def simpleHandler(testActor: ActorRef)(implicit system: ActorSystem): Handler[Any] =
      simpleHandler(testActor, simpleResultChecker)

    def simpleHandler[TErr, TResp](testActor: ActorRef, resultChecker: Any ⇒ Either[Option[TErr], TResp])(implicit system: ActorSystem): Handler[Any] =
      new GeneralActorRefHandler(nameFromActorRef(testActor), testActor, system)(resultChecker)

    def simpleHandlerProvider(testProbe: TestProbe)(implicit system: ActorSystem): HandlerProvider[Any] = {
      import system.dispatcher
      HandlerProvider.single(new GeneralActorRefHandler(nameFromActorRef(testProbe.ref), testProbe.ref, system)(simpleResultChecker))
    }

    class MockHandlerProvider(implicit val sys: ActorSystem) extends AgentHandlerProvider[Any] {
      override implicit def ex: ExecutionContext = sys.dispatcher

      def createHandler(testProbe: TestProbe = TestProbe()): Handler[Any] = {
        val newHandler: Handler[Any] = simpleHandler(testProbe)
        addHandler(newHandler)
        newHandler
      }

      def injectHandler(newHandler: Handler[Any]): Unit = {
        addHandler(newHandler)
      }

      def terminateHandler(handler: Handler[Any]): Unit = {
        removeHandler(handler)
      }

    }
  }

  trait MockActors {

    def processTimeBackend(processTime: FiniteDuration): Props = MockActors.delayedProcessorProps(processTime)

    def suicidal(delay: FiniteDuration): Props = Props(classOf[SuicidalActor], delay)
  }

  object MockActors {
    class SuicidalActor(delay: FiniteDuration) extends Actor {
      import context.dispatcher
      context.system.scheduler.scheduleOnce(delay, self, PoisonPill)
      def receive: Receive = PartialFunction.empty
    }

    class DelayedProcessor(processTime: FiniteDuration) extends Actor {
      import context.dispatcher
      override def receive: Receive = {
        case m ⇒
          context.system.scheduler.scheduleOnce(processTime, sender, Success)
      }
    }

    def delayedProcessorProps(processTime: FiniteDuration): Props = Props(new DelayedProcessor(processTime))
  }

  class MockReporter extends Reporter {
    private val metrics = new ConcurrentLinkedQueue[Metric]()
    def reported: List[Metric] = metrics.toArray(new Array[Metric](metrics.size())).toList
    override def report(metric: Metric): Unit =
      metrics.add(metric)

    override def withNewPrefix(modifier: (String) ⇒ String): Reporter = this
  }

}
