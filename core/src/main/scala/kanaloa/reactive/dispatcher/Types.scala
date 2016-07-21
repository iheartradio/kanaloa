package kanaloa.reactive.dispatcher

object Types {
  case class QueueLength(value: Int) extends AnyVal

  /**
   * Work done per milliseconds
   * @param value
   */
  case class Speed(value: Double) extends AnyVal
}
