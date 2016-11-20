package kanaloa

import kanaloa.metrics.{Metric, Reporter}
import java.util.concurrent.ConcurrentLinkedQueue

class MockReporter extends Reporter {
  private val metrics = new ConcurrentLinkedQueue[Metric]()
  def reported: List[Metric] = metrics.toArray(new Array[Metric](metrics.size())).toList
  override def report(metric: Metric): Unit =
    metrics.add(metric)
}
