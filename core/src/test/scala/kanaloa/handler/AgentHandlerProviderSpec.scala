package kanaloa.handler

import kanaloa.handler.AgentHandlerProviderSpec.{TestHandlerProvider, TestHandler}
import kanaloa.handler.HandlerProvider.{HandlersAdded, HandlerChange, Subscriber}
import org.scalatest.concurrent.{Eventually, AsyncAssertions}
import org.scalatest.{AsyncWordSpec, Matchers, WordSpecLike}

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
class AgentHandlerProviderSpec extends AsyncWordSpec with Matchers with Eventually {
  "AgentHandlerProvider#addHandler" should {
    "return true when the handler name is not" in {
      val h = new TestHandlerProvider
      val result = for {
        _ ← h.addHandler(new TestHandler("Hanlder1"))
        r ← h.addHandler(new TestHandler("Hanlder2"))
      } yield r
      result.map {
        _ should be(true)
      }
    }

    "return false when the handler name is duplicated" in {
      val h = new TestHandlerProvider
      val result = for {
        _ ← h.addHandler(new TestHandler("Hanlder1"))
        r ← h.addHandler(new TestHandler("Hanlder1"))
      } yield r
      result.map {
        _ should be(false)
      }
    }

    "broadcast handler added event" in {
      val h = new TestHandlerProvider
      @volatile
      var changes: List[HandlerChange] = Nil

      h.subscribe { c ⇒ changes = c :: changes }

      val newHandler = new TestHandler("Hanlder1")

      h.addHandler(newHandler).map { _ ⇒
        changes should contain(HandlersAdded(List(newHandler)))
      }

    }

    "does not broadcast handler added event if the name is duplicated" in {
      val h = new TestHandlerProvider
      @volatile
      var changes: List[HandlerChange] = Nil

      h.subscribe { c ⇒ changes = c :: changes }

      val rF = for {
        _ ← h.addHandler(new TestHandler("Hanlder1"))
        r ← h.addHandler(new TestHandler("Hanlder1"))
      } yield r

      rF.map { _ ⇒
        changes.length should be(1)
      }
    }
  }
}

object AgentHandlerProviderSpec {

  class TestHandlerProvider(implicit val ex: ExecutionContext) extends AgentHandlerProvider[String] {
    override def addHandler(handler: Handler[String]) =
      super.addHandler(handler)
  }

  class TestHandler(val name: String) extends Handler[String] {
    override type Resp = String
    override type Error = String

    override def handle(req: String): Handling[Resp, String] = ???
  }
}
