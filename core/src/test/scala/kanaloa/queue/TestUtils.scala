package kanaloa.queue

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import kanaloa._
import kanaloa.metrics.MetricsCollector

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
    metricsCollector: ActorRef,
    workSetting:      WorkSettings     = WorkSettings(),
    sendResultsTo:    Option[ActorRef] = None
  ): Props =
    Queue.ofIterator(iterator.map(DelegateeMessage(_)), metricsCollector, workSetting, sendResultsTo)

  class ScopeWithQueue(implicit system: ActorSystem) extends TestKit(system) with ImplicitSender {

    val delegatee = TestProbe()

    val backend = delegatee.ref

    def defaultProcessorProps(
      queue:            QueueRef,
      settings:         ProcessingWorkerPoolSettings = ProcessingWorkerPoolSettings(startingPoolSize = 1),
      metricsCollector: ActorRef                     = system.actorOf(MetricsCollector.props(None))
    ) = QueueProcessor.default(queue, backend, settings, metricsCollector)(resultChecker)
  }
}
