package kanaloa.reactive.dispatcher

import akka.actor.ActorRefFactory
import akka.testkit.{TestActors, TestProbe}
import kanaloa.reactive.dispatcher.queue._

import scala.concurrent.{ExecutionContext, Future}

trait Backends {
  def delayedBacked(implicit ex: ExecutionContext) = new Backend {
    def apply(f: ActorRefFactory): Future[QueueRef] = {
      Future {
        Thread.sleep(1000)
        f.actorOf(TestActors.echoActorProps)
      }
    }
  }
}
