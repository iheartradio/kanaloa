package kanaloa.reactive

package object dispatcher {
  type ResultChecker = PartialFunction[Any, Either[String, Any]]
}

