package kanaloa.util

object AnyEq {
  implicit final class AnyEqOps[A](a: A) {
    def ===(b: A) = a == b
  }
}
