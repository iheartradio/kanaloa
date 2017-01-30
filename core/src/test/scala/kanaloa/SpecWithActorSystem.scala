package kanaloa

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import kanaloa.TestUtils.Factories
import org.scalatest.concurrent.ScaledTimeSpans
import org.scalatest._

abstract class SpecWithActorSystem(_sys: ActorSystem) extends TestKit(_sys)
  with ImplicitSender with WordSpecLike with BeforeAndAfterAll with Matchers with ScaledTimeSpans {

  def this() = this(ActorSystem("Spec"))

  val shutdown: Tag = Tag("shutdown")

  implicit lazy val factories = new Factories()(_sys)

  override protected def afterAll(): Unit = {
    system.terminate()
  }
}
