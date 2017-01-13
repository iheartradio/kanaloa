package kanaloa.queue

import akka.actor.{ActorRefFactory, ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import kanaloa._
import kanaloa.handler.GeneralActorRefHandler.ResultChecker
import kanaloa.handler.HandlerProvider
import kanaloa.queue.WorkerPoolManager.{WorkerPoolSamplerFactory, WorkerFactory}

object TestUtils {

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

    val delegatee = TestProbe()

    val handlerProvider = HandlerProvider.actorRef("test", delegatee.ref)(resultChecker)

    def defaultWorkerPoolProps(
      queue:            QueueRef,
      settings:         WorkerPoolSettings = WorkerPoolSettings(startingPoolSize = 1),
      metricsCollector: ActorRef           = system.actorOf(WorkerPoolSampler.props(None, TestProbe().ref))
    ) = WorkerPoolManager.default(
      queue,
      TestHandlerProviders.simpleHandler(delegatee.ref, resultChecker),
      settings,
      WorkerFactory(None),
      new WorkerPoolSamplerFactory {
        def apply(handlerName: String)(implicit ac: ActorRefFactory): ActorRef = metricsCollector
      },
      None
    )
  }
}
