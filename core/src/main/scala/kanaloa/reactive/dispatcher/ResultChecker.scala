package kanaloa.reactive.dispatcher

import scala.reflect._

object ResultChecker {

  /**
   * Result checker based on type
   * @tparam ExpectedResultT
   * @return
   */
  def expectType[ExpectedResultT: ClassTag]: ResultChecker = {
    case t: ExpectedResultT if classTag[ExpectedResultT].runtimeClass.isInstance(t) ⇒ Right(t)
  }

  /**
   * always return success no mater what
   */
  val complacent: ResultChecker = {
    case m ⇒ Right(m)
  }
}
