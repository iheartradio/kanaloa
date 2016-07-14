package kanaloa.reactive.dispatcher

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.concurrent.ScaledTimeSpans
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpec, WordSpecLike}

abstract class SpecWithActorSystem(_sys: ActorSystem) extends TestKit(_sys)
  with ImplicitSender with WordSpecLike with BeforeAndAfterAll with ShouldMatchers with ScaledTimeSpans {

  def this() = this(ActorSystem("Spec"))

  override protected def afterAll(): Unit = {
    system.terminate()
  }
}
