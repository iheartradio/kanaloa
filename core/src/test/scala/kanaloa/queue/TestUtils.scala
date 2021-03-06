package kanaloa.queue

import akka.actor.{ActorRefFactory, ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import kanaloa._
import kanaloa.handler.GeneralActorRefHandler.ResultChecker
import kanaloa.handler.HandlerProvider
import kanaloa.metrics.Reporter
import kanaloa.queue.Sampler.SamplerSettings
import kanaloa.queue.WorkerPoolManager.{WorkerPoolSamplerFactory, WorkerFactory}

object QueueTestUtils {

  case class DelegateeMessage(msg: String)
  case class MessageProcessed(msg: String)
  case object MessageFailed

  val resultChecker: ResultChecker[String, String] = {
    case MessageProcessed(msg) ⇒ Right(msg)
    case m                     ⇒ Left(Some(s"unrecognized message received by resultChecker: $m (${m.getClass})"))
  }

  def iteratorQueueProps(
    iterator:         Iterator[String],
    metricsCollector: ActorRef,
    workSetting:      WorkSettings     = WorkSettings(),
    sendResultsTo:    Option[ActorRef] = None
  ): Props =
    Queue.ofIterator(iterator.map(DelegateeMessage(_)), metricsCollector, workSetting, sendResultsTo)

  class ScopeWithQueue(implicit system: ActorSystem) extends TestKit(system) with ImplicitSender {

    val service = TestProbe()

    val handlerProvider = HandlerProvider.actorRef("test", service.ref)(resultChecker)

    def defaultWorkerPoolProps(
      queue:            QueueRef,
      settings:         WorkerPoolSettings = WorkerPoolSettings(startingPoolSize = 1),
      metricsCollector: ActorRef           = system.actorOf(WorkerPoolSampler.props(None, TestProbe().ref, SamplerSettings(), None))
    ) = WorkerPoolManager.default(
      queue,
      TestUtils.HandlerProviders.simpleHandler(service.ref, resultChecker),
      settings,
      WorkerFactory.default,
      new WorkerPoolSamplerFactory {
        def apply(reporter: Option[Reporter], mft: Option[ActorRef])(implicit ac: ActorRefFactory): ActorRef = metricsCollector
      },
      None,
      None,
      None
    )
  }
}
