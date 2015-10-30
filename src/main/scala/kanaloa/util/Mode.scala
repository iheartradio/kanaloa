package kanaloa.util

import scala.language.higherKinds
import scala.reflect.ClassTag
import scala.util.{ Try, Success, Failure }

// based on http://blog.scalac.io/2015/05/28/scala-modes.html
trait Mode {
  type Type[+_]

  /** Wrap all exceptions */
  def wrap[T](body: ⇒ T): Type[T] = wrapException[T, Exception](body)

  /** Wrap a specific type of exception -- all other exception types will throw */
  def wrapException[T, E <: Exception: ClassTag](body: ⇒ T): Type[T]
}

object Mode {
  /** uncaughtMode doesn't handle exceptions */
  val uncaughtMode = new Mode {
    type Type[+T] = T

    def wrapException[T, E <: Exception: ClassTag](body: ⇒ T): Type[T] = body
  }

  /** optionMode wraps the result in an Option, if exception type matches */
  val optionMode = new Mode {
    type Type[+T] = Option[T]

    def wrapException[T, E <: Exception: ClassTag](body: ⇒ T): Type[T] =
      try { Option(body) } catch { case e: E ⇒ None }
  }

  /** tryMode wraps the result in a Try, for all exception types */
  val tryMode = new Mode {
    type Type[+T] = Try[T]

    def wrapException[T, E <: Exception: ClassTag](body: ⇒ T): Type[T] = Try(body)
  }
}

