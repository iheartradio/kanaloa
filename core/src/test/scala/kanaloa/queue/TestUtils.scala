package kanaloa.queue

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import kanaloa._
import kanaloa.handler.GeneralActorRefHandler.ResultChecker
import kanaloa.handler.HandlerProvider
import kanaloa.metrics.MetricsCollector

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

    def defaultProcessorProps(
      queue:            QueueRef,
      settings:         ProcessingWorkerPoolSettings = ProcessingWorkerPoolSettings(startingPoolSize = 1),
      metricsCollector: ActorRef                     = system.actorOf(MetricsCollector.props(None))
    ) = QueueProcessor.default(queue, handlerProvider, settings, metricsCollector)
  }
}
