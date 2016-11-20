

package object kanaloa {
  type ResultChecker = PartialFunction[Any, Either[String, Any]]
}
