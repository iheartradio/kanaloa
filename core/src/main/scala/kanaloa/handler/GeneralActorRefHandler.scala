package kanaloa.handler

import java.util.concurrent.atomic.{AtomicLong, AtomicBoolean, AtomicInteger}

import akka.actor.Actor.Receive
import akka.actor._
import kanaloa._
import kanaloa.util.AtomicCyclicInt

import scala.concurrent.duration.Duration
import scala.concurrent.{Promise, Future}
import scala.util.Success

/**
 * Handler based on general actors who do not have ability to provide pressure information or instructions back to users.
 *
 * @param name
 * @param actor
 * @param factory
 * @param resultChecker
 */
class GeneralActorRefHandler[TResp, TError](
  val name: String,
  actor:    ActorRef,
  factory:  ActorRefFactory
)(resultChecker: Any ⇒ Either[Option[TError], TResp]) extends Handler[Any] {
  import GeneralActorRefHandler._

  override type Error = Option[TError]
  override type Resp = TResp

  val index = new AtomicCyclicInt(0, 10000000)
  val cancelled = new AtomicBoolean(false)

  override def handle(req: Any): Handling[Resp, Error] = new Handling[Resp, Error] {
    val promise: Promise[Either[Interrupted, Any]] = Promise()

    val handlerActor = factory.actorOf(
      handlerActorProps(promise, actor),
      s"${name}-handler-of-${actor.path.name}_${index.incrementAndGet()}"
    )

    import factory.dispatcher

    override val result: Future[Result[Resp, Error]] = promise.future.map {
      case Left(TargetTerminated) ⇒ Result(Left(None), Some(Terminate))
      case Left(Cancelled)        ⇒ Result(Left(None), None)
      case Right(m)               ⇒ Result(resultChecker(m), None)
    }

    override val cancellable: Option[Cancellable] = Some(new Cancellable {
      def cancel(): Boolean = {
        val shouldCancel = cancelled.compareAndSet(false, true)
        if (shouldCancel)
          handlerActor ! Cancel
        shouldCancel
      }
    })
  }

}

object GeneralActorRefHandler {
  private class HandlerActor(promise: Promise[Either[Interrupted, Any]], target: ActorRef) extends Actor {
    context watch target

    def complete(r: Either[Interrupted, Any]): Unit = {
      promise.tryComplete(Success(r))
      context stop self
    }

    override def receive: Receive = {
      case Cancel ⇒
        complete(Left(Cancelled))

      case Terminated(`target`) ⇒
        complete(Left(TargetTerminated))

      case x ⇒
        complete(Right(x))
    }
  }

  private def handlerActorProps(promise: Promise[Either[Interrupted, Any]], target: ActorRef): Props = Props(new HandlerActor(promise, target))

  private case object Cancel

  sealed abstract class Interrupted extends Product with Serializable

  case object TargetTerminated extends Interrupted
  case object Cancelled extends Interrupted
}
