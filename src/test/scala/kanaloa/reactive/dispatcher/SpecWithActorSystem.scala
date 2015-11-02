package kanaloa.reactive.dispatcher

import akka.actor.ActorSystem
import org.specs2.mutable.Specification
import org.specs2.specification.AfterAll

trait SpecWithActorSystem extends Specification with AfterAll {
  sequential
  implicit lazy val system = ActorSystem()

  def afterAll(): Unit = system.terminate()
}
