package kanaloa.reactive.dispatcher.util

import kanaloa.util.FiniteCollection
import kanaloa.util.FiniteCollection._
import org.specs2.mutable.Specification

class FiniteCollectionSpec extends Specification {

  "TakeRightWhile" >> {
    "takes the right part and keep the order" >> {
      Vector(1, 4, 2, 3, 1).takeRightWhile(_ < 4) === Vector(2, 3, 1)
    }

    "returns empty when failed the first test" >> {
      Vector(1, 4, 1).takeRightWhile(_ < 0) === Vector[Int]()
    }

  }

}
