package kanaloa.reactive.dispatcher

import akka.actor.Actor.Receive
import akka.actor._
import akka.testkit.{TestActors, TestProbe}
import kanaloa.reactive.dispatcher.Backends.SuicidalActor
import kanaloa.reactive.dispatcher.IntegrationTests.Success
import kanaloa.reactive.dispatcher.queue._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Promise, ExecutionContext, Future}

trait Backends {
  def promiseBackend(promise: Promise[ActorRef])(implicit ex: ExecutionContext) = new Backend {
    def apply(f: ActorRefFactory): Future[ActorRef] = promise.future
  }

  def processTimeBackend(processTime: FiniteDuration): Backend = Backends.delayedProcessorProps(processTime)

  def suicidal(delay: FiniteDuration): Backend = Props(classOf[SuicidalActor], delay)
}

object Backends {
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
