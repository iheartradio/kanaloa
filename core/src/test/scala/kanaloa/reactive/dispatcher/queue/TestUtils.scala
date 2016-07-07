package kanaloa.reactive.dispatcher.queue

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import kanaloa.reactive.dispatcher._
import kanaloa.reactive.dispatcher.metrics.MetricsCollector

object TestUtils {

  case class DelegateeMessage(msg: String)
  case class MessageProcessed(msg: String)
  case object MessageFailed

  val resultChecker: ResultChecker = {
    case MessageProcessed(msg) ⇒ Right(msg)
    case m                     ⇒ Left(s"unrecognized message received by resultChecker: $m (${m.getClass})")
  }

  def iteratorQueueProps(
    iterator:         Iterator[String],
    historySettings:  DispatchHistorySettings = DispatchHistorySettings(),
    workSetting:      WorkSettings            = WorkSettings(),
    sendResultsTo:    Option[ActorRef]        = None,
    metricsCollector: MetricsCollector        = new MetricsCollector(None)
  ): Props =
    Queue.ofIterator(iterator.map(DelegateeMessage(_)), historySettings, workSetting, sendResultsTo, metricsCollector)

  class ScopeWithQueue(implicit system: ActorSystem) extends TestKit(system) with ImplicitSender {

    val delegatee = TestProbe()

    val backend = delegatee.ref

    def defaultProcessorProps(
      queue:            QueueRef,
      settings:         ProcessingWorkerPoolSettings = ProcessingWorkerPoolSettings(startingPoolSize = 1),
      metricsCollector: MetricsCollector             = new MetricsCollector(None)
    ) = QueueProcessor.default(queue, backend, settings, metricsCollector)(resultChecker)
  }
}
