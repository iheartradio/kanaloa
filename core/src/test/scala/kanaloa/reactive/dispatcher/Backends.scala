package kanaloa.reactive.dispatcher

import akka.actor.{ActorRef, ActorRefFactory}
import akka.testkit.{TestActors, TestProbe}
import kanaloa.reactive.dispatcher.queue._

import scala.concurrent.{Promise, ExecutionContext, Future}

trait Backends {
  def promiseBackend(promise: Promise[ActorRef])(implicit ex: ExecutionContext) = new Backend {
    def apply(f: ActorRefFactory): Future[ActorRef] = promise.future
  }

}
