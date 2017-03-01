package kanaloa.util

object Naming {
  def sanitizeActorName(s: String) = s.replaceAll("[^A-Za-z0-9()\\[\\]]", "_")
}
