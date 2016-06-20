package kanaloa.reactive.dispatcher.util

import kanaloa.util.FiniteCollection._
import org.scalatest.{ShouldMatchers, WordSpec}

class FiniteCollectionSpec extends WordSpec with ShouldMatchers {

  "TakeRightWhile" should {

    "takes the right part and keep the order" in {
      Vector(1, 4, 2, 3, 1).takeRightWhile(_ < 4) shouldBe Vector(2, 3, 1)
    }

    "returns empty when failed the first test" in {
      Vector(1, 4, 1).takeRightWhile(_ < 0) shouldBe Vector[Int]()
    }
  }
}
