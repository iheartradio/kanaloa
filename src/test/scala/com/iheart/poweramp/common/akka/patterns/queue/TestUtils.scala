package com.iheart.poweramp.common.akka.patterns.queue

import akka.actor.{ActorSystem, Props, Actor, ActorRef}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.iheart.poweramp.common.akka.patterns.queue.QueueProcessor.Settings
import com.iheart.poweramp.common.akka.patterns.queue.Worker.{WorkSetting, ResultChecker}
import org.specs2.specification.Scope

object TestUtils {

  case class DelegateeMessage(msg: String)
  case class MessageProcessed(msg: String)
  case object MessageFailed

  class Wrapper(prob: ActorRef) extends Actor {
    def receive = {
      case m => prob forward m
    }
  }

  object Wrapper {
    def props(prob: ActorRef): Props = Props(new Wrapper(prob))
  }

  val resultChecker: ResultChecker = {
    case MessageProcessed(msg) => Right(msg)
    case m => Left(s"unrecognized message received by resultChecker: $m (${m.getClass})")
  }

  def iteratorQueueProps(iterator: Iterator[String], workSetting: WorkSetting = WorkSetting()): Props =
    Queue.ofIterator(iterator.map(DelegateeMessage(_)), workSetting)

  class ScopeWithQueue(implicit system: ActorSystem) extends TestKit(system) with ImplicitSender with Scope {

    val delegatee = TestProbe()

    val delegateeProps = Wrapper.props(delegatee.ref)

    def defaultProcessorProps(queue: QueueRef, settings: Settings = Settings(numOfWorkers = 1)) = QueueProcessor.default(queue, delegateeProps, settings)(resultChecker)
  }
}
