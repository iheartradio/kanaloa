package com.iheart.poweramp.common.akka.patterns.queue

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import akka.actor._
import akka.testkit.TestActorRef
import com.iheart.poweramp.common.akka.SpecWithActorSystem
import com.iheart.poweramp.common.akka.patterns.CommonProtocol.QueryStatus
import com.iheart.poweramp.common.akka.patterns.queue.Queue._
import scala.concurrent.duration._

import TestUtils._


class QueueWithBackPressureSpec extends SpecWithActorSystem {

  "records history correctly" in new QueueScope {
    val q = withBackPressure()
    initQueue(q, numberOfWorkers = 2)

    waitForWorkerRegistration(q, 2)

    q ! Enqueue("a", self)
    expectMsg(WorkEnqueued)
    q ! QueryStatus()
    val qs = expectMsgType[QueueStatus]
    qs.queuedWorkers.size === 1
    qs.bufferHistory.last.numInBuffer === 0
    qs.bufferHistory.map(_.dispatched).sum === 1

    q ! Enqueue("b", self)
    expectMsg(WorkEnqueued)
    q ! QueryStatus()
    val qs2 = expectMsgType[QueueStatus]
    qs2.queuedWorkers.size === 0

    qs2.bufferHistory.last.numInBuffer === 0
    qs2.bufferHistory.map(_.dispatched).sum === 2

    q ! Enqueue("c", self)
    expectMsg(WorkEnqueued)
    q ! QueryStatus()
    val qs3 = expectMsgType[QueueStatus]

    qs3.queuedWorkers.size === 0
    qs3.bufferHistory.last.numInBuffer === 1
    qs3.bufferHistory.map(_.dispatched).sum === 2
    delegatee.expectMsg("a")
    delegatee.expectMsg("b")

    delegatee.reply(MessageProcessed("a"))
    delegatee.expectMsg("c")
    expectNoMsg(50.milliseconds)

    q ! QueryStatus()
    val qs4 = expectMsgType[QueueStatus]

    qs4.queuedWorkers.size === 0
    qs4.workBuffer.size === 0
    qs4.bufferHistory.last.numInBuffer === 0
    qs4.bufferHistory.map(_.dispatched).sum === 3

  }

  "isOverCapacity" >> {

    val q = TestActorRef[QueueWithBackPressure](Queue.withBackPressure(BackPressureSettings(maxBufferSize = 10,
                                                                        thresholdForExpectedWaitTime = 5.minutes))).underlyingActor

    def status(ps: (Int, Int, LocalDateTime)*): QueueStatus = QueueStatus(bufferHistory =  ps.map { case (dispatched, size, time) => BufferHistoryEntry(dispatched, size, time) }.toVector )

    "return false if there is no entries" >> {
      q.isOverCapacity(status()) must beFalse
    }

    "return false if there is only entries with 0 buffer" >> {
      q.isOverCapacity(status(
        (1, 0, LocalDateTime.now.minusMinutes(2)),
        (1, 0, LocalDateTime.now.minusMinutes(1)),
        (1, 0, LocalDateTime.now)
      )) must beFalse
    }

    "return true if the most recent size is larger than max buffer size" >> {
      q.isOverCapacity(status(
        (1, 0, LocalDateTime.now.minusMinutes(2)),
        (1, 0, LocalDateTime.now.minusMinutes(1)),
        (1, 11, LocalDateTime.now)
      )) must beTrue
    }

    "return false if most recent size is less than max buffer size" >> {
      q.isOverCapacity(status(
        (1, 0, LocalDateTime.now.minusMinutes(2)),
        (1, 0, LocalDateTime.now.minusMinutes(1)),
        (1, 8, LocalDateTime.now)
      )) must beFalse
    }

    "return false if wait time is within 5 minute" >> {
      q.isOverCapacity(status(
        (1, 3, LocalDateTime.now.minusMinutes(2)),
        (1, 3, LocalDateTime.now.minusMinutes(1)),
        (1, 3, LocalDateTime.now) // 3 left at server rate 1 per minute
      )) must beFalse
    }

    "return true if wait time is more than 5 minute" >> {
      q.isOverCapacity(status(
        (0, 8, LocalDateTime.now.minusMinutes(2)),
        (1, 7, LocalDateTime.now.minusMinutes(1)),
        (1, 6, LocalDateTime.now) // 6 left at server rate 1 per minute
      )) must beTrue
    }

    "return true for long queue but not enough dispatch lately at all" >> {
      q.isOverCapacity(status(
        (0, 4, LocalDateTime.now.minusMinutes(3)),
        (0, 5, LocalDateTime.now.minusMinutes(2)),
        (0, 6, LocalDateTime.now.minusMinutes(1)),
        (0, 7, LocalDateTime.now) // 6 left at server rate 1 per minute
      )) must beTrue
    }

    "return false when just started to pickup traffic" >> {
      q.isOverCapacity(status(
        (0, 9, LocalDateTime.now.minus(2, ChronoUnit.MILLIS)),
        (0, 9, LocalDateTime.now.minus(1, ChronoUnit.MILLIS)),
        (0, 9, LocalDateTime.now) // 6 left at server rate 1 per minute
      )) must beFalse
    }

    "performance test" >> {
      val hist = status(
        (1, 8, LocalDateTime.now.minusMinutes(2)),
        (1, 7, LocalDateTime.now.minusMinutes(2)),
        (3, 10, LocalDateTime.now.minusMinutes(2)),
        (1, 8, LocalDateTime.now.minusMinutes(2)),
        (1, 12, LocalDateTime.now.minusMinutes(2)),
        (1, 8, LocalDateTime.now.minusMinutes(2)),
        (1, 5, LocalDateTime.now.minusMinutes(2)),
        (3, 8, LocalDateTime.now.minusMinutes(2)),
        (1, 4, LocalDateTime.now.minusMinutes(2)),
        (1, 8, LocalDateTime.now.minusMinutes(2)),
        (1, 8, LocalDateTime.now.minusMinutes(2)),
        (0, 7, LocalDateTime.now.minusMinutes(2)),
        (0, 8, LocalDateTime.now.minusMinutes(2)),
        (1, 8, LocalDateTime.now.minusMinutes(2)),
        (2, 10, LocalDateTime.now.minusMinutes(2)),
        (1, 8, LocalDateTime.now.minusMinutes(2)),
        (4, 6, LocalDateTime.now.minusMinutes(2)),
        (1, 7, LocalDateTime.now.minusMinutes(1)),
        (1, 6, LocalDateTime.now)
      )
      val start = System.nanoTime()
      val sampleSize = 10000
      (1 to sampleSize).foreach(_ =>
        q.isOverCapacity(hist)
      )

      val duration = (System.nanoTime() - start) / 1000
      val speed = duration / sampleSize
      speed.toInt must be_<(20) //per test taking less than 20 microseconds (on a macbook pro it's taking 5-6 microseconds)
    }
  }
}
