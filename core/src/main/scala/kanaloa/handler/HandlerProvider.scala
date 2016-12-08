package kanaloa.handler

import kanaloa.handler.HandlerProvider.HandlerChange

trait HandlerProvider[T] {
  def handlers: List[Handler[T]]
  def subscribe(f: HandlerChange ⇒ Unit)

}

object HandlerProvider {
  sealed abstract class HandlerChange extends Product with Serializable
  case class HandlersAdded(newHandlers: List[Handler[_]]) extends HandlerChange
  case class HandlersRemoved(removedHandlers: List[Handler[_]]) extends HandlerChange
}
