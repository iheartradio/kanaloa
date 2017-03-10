package kanaloa.util

import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec

class AtomicCyclicInt(val init: Int = 0, val maxVal: Int = 10000) {

  private val ai = new AtomicInteger(init)

  def incrementAndGet(): Int = {
    @tailrec
    def loop(): Int = {
      val curVal = ai.get
      val newVal = (curVal + 1) % maxVal
      if (!ai.compareAndSet(curVal, newVal)) loop()
      else newVal
    }
    loop()
  }
}
