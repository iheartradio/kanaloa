package kanaloa.reactive.dispatcher

import akka.actor.ActorRefFactory
import akka.testkit.{TestActors, TestProbe}
import kanaloa.reactive.dispatcher.queue._

import scala.concurrent.{ExecutionContext, Future}

trait Backends {
  def delayedBackend(delayInMs: Long = 1000)(implicit ex: ExecutionContext) = new Backend {
    def apply(f: ActorRefFactory): Future[QueueRef] = {
      Future {
        Thread.sleep(delayInMs)
        f.actorOf(TestActors.echoActorProps)
      }
    }
  }
}
