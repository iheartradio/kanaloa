package kanaloa.handler

import java.util.concurrent.atomic.{AtomicLong, AtomicBoolean, AtomicInteger}

import akka.actor.Actor.Receive
import akka.actor._
import kanaloa._
import kanaloa.handler.GeneralActorRefHandler.ResultChecker
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
  val name:  String,
  val actor: ActorRef,
  factory:   ActorRefFactory
)(resultChecker: ResultChecker[TResp, TError]) extends Handler[Any] {
  import GeneralActorRefHandler._

  override type Error = Option[TError]
  override type Resp = TResp

  val maxConcurrentHandling = 100000000 //the actual max concurrent handling is controlled by the kanaloa, this number is just a hard limit, not suggesting that the concurrent handling should be allowed to go there.
  val index = new AtomicCyclicInt(0, maxConcurrentHandling)

  override def handle(req: Any): Handling[Resp, Error] = new Handling[Resp, Error] {
    val promise: Promise[Either[Interrupted, Any]] = Promise()
    val cancelled = new AtomicBoolean(false)
    val handlerActor = factory.actorOf(
      handlerActorProps(promise, actor, cancelled),
      s"${name}-handler-of-${actor.path.name}_${index.incrementAndGet()}"
    )

    (actor ! req)(handlerActor)

    import factory.dispatcher

    override val result: Future[Result[Resp, Error]] = promise.future.map {
      case Left(TargetTerminated) ⇒
        Result(Left(None), Some(Terminate))
      case Left(Cancelled) ⇒ Result(Left(None), None)
      case Right(m)        ⇒ Result(resultChecker(m), None)
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
  type ResultChecker[TResp, TError] = Any ⇒ Either[Option[TError], TResp]

  private class HandlerActor(promise: Promise[Either[Interrupted, Any]], target: ActorRef, cancelled: AtomicBoolean) extends Actor {
    context watch target

    def complete(r: Either[Interrupted, Any]): Unit = {
      cancelled.set(true)
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

  private def handlerActorProps(promise: Promise[Either[Interrupted, Any]], target: ActorRef, cancelled: AtomicBoolean): Props = Props(new HandlerActor(promise, target, cancelled))

  private case object Cancel

  sealed abstract class Interrupted extends Product with Serializable

  case object TargetTerminated extends Interrupted
  case object Cancelled extends Interrupted

  def apply[TResp, TError](
    name:    String,
    actor:   ActorRef,
    factory: ActorRefFactory
  )(resultChecker: ResultChecker[TResp, TError]) = new GeneralActorRefHandler[TResp, TError](name, actor, factory)(resultChecker)

}

