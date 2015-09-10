package com.iheart.workpipeline.collection

object FiniteCollection {


  implicit class FiniteQueue[A](val self: Vector[A]) extends AnyVal {

    def enqueueFinite[B >: A](elem: => B, maxSize: Int): Vector[B] = {
      if(maxSize > 0) {
        val ret = self :+ elem
        if(ret.size > maxSize) ret.takeRight(maxSize) else ret
      } else Vector.empty[B]
    }

    def dequeueOption: Option[(A, Vector[A])] = {
      if(self.isEmpty) None else Some((self.head, self.tail))
    }

    def takeRightWhile(p: A => Boolean): Vector[A] = {
      var result = Vector.empty[A]
      val iter = self.reverseIterator
      while(iter.hasNext) {
        val next = iter.next()
        if(p(next))
          result = next +: result
        else
          return result
      }
      return result
    }
  }
}

