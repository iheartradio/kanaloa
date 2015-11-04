package kanaloa.reactive.dispatcher.queue

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import akka.actor._
import akka.testkit.TestActorRef
import kanaloa.reactive.dispatcher.ApiProtocol._
import kanaloa.reactive.dispatcher.queue.Queue._
import kanaloa.reactive.dispatcher.queue.TestUtils._
import kanaloa.reactive.dispatcher.{ ApiProtocol, SpecWithActorSystem }

import scala.concurrent.duration._

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
    qs.bufferHistory.last.queueLength === 0
    qs.bufferHistory.map(_.dispatched).sum === 1

    q ! Enqueue("b", self)
    expectMsg(WorkEnqueued)
    q ! QueryStatus()
    val qs2 = expectMsgType[QueueStatus]
    qs2.queuedWorkers.size === 0

    qs2.bufferHistory.last.queueLength === 0
    qs2.bufferHistory.map(_.dispatched).sum === 2

    q ! Enqueue("c", self)
    expectMsg(WorkEnqueued)
    q ! QueryStatus()
    val qs3 = expectMsgType[QueueStatus]

    qs3.queuedWorkers.size === 0
    qs3.bufferHistory.last.queueLength === 1
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
    qs4.bufferHistory.last.queueLength === 0
    qs4.bufferHistory.map(_.dispatched).sum === 3

  }

  "isOverCapacity" >> {

    val q = TestActorRef[QueueWithBackPressure](Queue.withBackPressure(BackPressureSettings(
      maxBufferSize = 10,
      thresholdForExpectedWaitTime = 5.minutes
    ))).underlyingActor

    def status(ps: (Int, Int, LocalDateTime)*): QueueStatus = QueueStatus(bufferHistory = ps.map { case (dispatched, size, time) ⇒ BufferHistoryEntry(dispatched, size, time) }.toVector)

    "return false if there is no entries" >> {
      q.checkCapacity(status()) must beFalse
    }

    "return false if there is only entries with 0 buffer" >> {
      q.checkCapacity(status(
        (1, 0, LocalDateTime.now.minusMinutes(2)),
        (1, 0, LocalDateTime.now.minusMinutes(1)),
        (1, 0, LocalDateTime.now)
      )) must beFalse
    }

    "return true if the most recent size is larger than max buffer size" >> {
      q.checkCapacity(status(
        (1, 0, LocalDateTime.now.minusMinutes(2)),
        (1, 0, LocalDateTime.now.minusMinutes(1)),
        (1, 11, LocalDateTime.now)
      )) must beTrue
    }

    "return false if most recent size is less than max buffer size" >> {
      q.checkCapacity(status(
        (1, 0, LocalDateTime.now.minusMinutes(2)),
        (1, 0, LocalDateTime.now.minusMinutes(1)),
        (1, 8, LocalDateTime.now)
      )) must beFalse
    }

    "return false if wait time is within 5 minute" >> {
      q.checkCapacity(status(
        (1, 3, LocalDateTime.now.minusMinutes(2)),
        (1, 3, LocalDateTime.now.minusMinutes(1)),
        (1, 3, LocalDateTime.now) // 3 left at server rate 1 per minute
      )) must beFalse
    }

    "return true if wait time is more than 5 minute" >> {
      q.checkCapacity(status(
        (0, 8, LocalDateTime.now.minusMinutes(2)),
        (1, 7, LocalDateTime.now.minusMinutes(1)),
        (1, 6, LocalDateTime.now) // 6 left at server rate 1 per minute
      )) must beTrue
    }

    "return true for long queue but not enough dispatch lately at all" >> {
      q.checkCapacity(status(
        (0, 4, LocalDateTime.now.minusMinutes(3)),
        (0, 5, LocalDateTime.now.minusMinutes(2)),
        (0, 6, LocalDateTime.now.minusMinutes(1)),
        (0, 7, LocalDateTime.now) // 6 left at server rate 1 per minute
      )) must beTrue
    }

    "return false when just started to pickup traffic" >> {
      q.checkCapacity(status(
        (0, 9, LocalDateTime.now.minus(2, ChronoUnit.MILLIS)),
        (0, 9, LocalDateTime.now.minus(1, ChronoUnit.MILLIS)),
        (0, 9, LocalDateTime.now) // 6 left at server rate 1 per minute
      )) must beFalse
    }

  }
}
