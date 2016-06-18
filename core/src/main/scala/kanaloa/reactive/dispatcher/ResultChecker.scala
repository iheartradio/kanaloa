package kanaloa.reactive.dispatcher

import scala.reflect._

object ResultChecker {

  def simple[ExpectedResultT: ClassTag]: ResultChecker = {
    case t: ExpectedResultT if classTag[ExpectedResultT].runtimeClass.isInstance(t) ⇒ Right(t)

  }
}
