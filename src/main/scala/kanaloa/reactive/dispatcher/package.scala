package kanaloa.reactive

import akka.actor.{ ActorRef, ActorRefFactory }

package object dispatcher {
  type ResultChecker = PartialFunction[Any, Either[String, Any]]
  type Backend = ActorRefFactory ⇒ ActorRef

}
