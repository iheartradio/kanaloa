package kanaloa.reactive.dispatcher.queue

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import akka.actor._
import akka.testkit.TestActorRef
import kanaloa.reactive.dispatcher.ApiProtocol._
import kanaloa.reactive.dispatcher.SpecWithActorSystem
import kanaloa.reactive.dispatcher.queue.Queue._
import kanaloa.reactive.dispatcher.queue.TestUtils._

import scala.concurrent.duration._

class QueueWithBackPressureSpec extends SpecWithActorSystem {

  "Queue with Backpressure" should {

    "records history correctly" in new QueueScope {
      val q = withBackPressure()
      initQueue(q, numberOfWorkers = 2)

      waitForWorkerRegistration(q, 2)

      q ! Enqueue("a", self)
      expectMsg(WorkEnqueued)
      q ! QueryStatus()
      val qs = expectMsgType[QueueStatus]
      qs.queuedWorkers.size === 1
      qs.dispatchHistory.last.queueLength === 0
      qs.dispatchHistory.map(_.dispatched).sum === 1

      q ! Enqueue("b", self)
      expectMsg(WorkEnqueued)
      q ! QueryStatus()
      val qs2 = expectMsgType[QueueStatus]
      qs2.queuedWorkers.size === 0

      qs2.dispatchHistory.last.queueLength === 0
      qs2.dispatchHistory.map(_.dispatched).sum === 2

      q ! Enqueue("c", self)
      expectMsg(WorkEnqueued)
      q ! QueryStatus()
      val qs3 = expectMsgType[QueueStatus]

      qs3.queuedWorkers.size === 0
      qs3.dispatchHistory.last.queueLength === 1
      qs3.dispatchHistory.map(_.dispatched).sum === 2
      delegatee.expectMsg("a")
      delegatee.expectMsg("b")

      delegatee.reply(MessageProcessed("a"))
      delegatee.expectMsg("c")
      expectNoMsg(50.milliseconds)

      q ! QueryStatus()
      val qs4 = expectMsgType[QueueStatus]

      qs4.queuedWorkers.size === 0
      qs4.workBuffer.size === 0
      qs4.dispatchHistory.last.queueLength === 0
      qs4.dispatchHistory.map(_.dispatched).sum === 3

    }
  }

  "isOverCapacity" should {

    val q = TestActorRef[QueueWithBackPressure](Queue.withBackPressure(
      DispatchHistorySettings(),
      BackPressureSettings(
        maxBufferSize = 10,
        thresholdForExpectedWaitTime = 5.minutes
      )
    )).underlyingActor

    def status(stats: (Int, Int, Int, LocalDateTime)*): QueueStatus =
      QueueStatus(dispatchHistory = stats.map(DispatchHistoryEntry.tupled).toVector)

    def statusWithQueueSize(queueSizes: (Int, LocalDateTime)*): QueueStatus =
      status(queueSizes.map {
        case (queueSize, time) ⇒ (1, queueSize, 1, time)
      }: _*)

    "return false if there is no entries" in {
      q.checkOverCapacity(status()) shouldBe false
    }

    "return false if there is only entries with 0 buffer" in {
      q.checkOverCapacity(statusWithQueueSize(
        (0, LocalDateTime.now.minusMinutes(2)),
        (0, LocalDateTime.now.minusMinutes(1)),
        (0, LocalDateTime.now)
      )) shouldBe false
    }

    "return true if the most recent size is larger than max buffer size" in {
      q.checkOverCapacity(statusWithQueueSize(
        (0, LocalDateTime.now.minusMinutes(2)),
        (0, LocalDateTime.now.minusMinutes(1)),
        (11, LocalDateTime.now)
      )) shouldBe true
    }

    "return false if most recent size is less than max buffer size" in {
      q.checkOverCapacity(statusWithQueueSize(
        (0, LocalDateTime.now.minusMinutes(2)),
        (0, LocalDateTime.now.minusMinutes(1)),
        (8, LocalDateTime.now)
      )) shouldBe false
    }

    "return false if wait time is within 5 minute" in {
      q.checkOverCapacity(status(
        (1, 3, 1, LocalDateTime.now.minusMinutes(2)),
        (1, 3, 1, LocalDateTime.now.minusMinutes(1)),
        (1, 3, 1, LocalDateTime.now) // 3 left at server rate 1 per minute
      )) shouldBe false
    }

    "return true if wait time is more than 5 minute" in {
      q.checkOverCapacity(status(
        (0, 8, 0, LocalDateTime.now.minusMinutes(2)),
        (1, 7, 0, LocalDateTime.now.minusMinutes(1)),
        (1, 6, 0, LocalDateTime.now) // 6 left at server rate 1 per minute
      )) shouldBe true
    }

    "return true for long queue but not enough dispatch lately at all" in {
      q.checkOverCapacity(status(
        (0, 4, 0, LocalDateTime.now.minusMinutes(3)),
        (0, 5, 0, LocalDateTime.now.minusMinutes(2)),
        (0, 6, 0, LocalDateTime.now.minusMinutes(1)),
        (0, 7, 0, LocalDateTime.now) // 6 left at server rate 1 per minute
      )) shouldBe true
    }

    "return false when just started to pickup traffic" in {
      q.checkOverCapacity(status(
        (0, 9, 1, LocalDateTime.now.minus(2, ChronoUnit.MILLIS)),
        (0, 9, 1, LocalDateTime.now.minus(1, ChronoUnit.MILLIS)),
        (0, 9, 1, LocalDateTime.now) // 6 left at server rate 1 per minute
      )) shouldBe false
    }

  }
}
