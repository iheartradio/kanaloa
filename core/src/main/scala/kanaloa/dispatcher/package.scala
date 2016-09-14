package kanaloa

package object dispatcher {
  type ResultChecker = PartialFunction[Any, Either[String, Any]]
}
