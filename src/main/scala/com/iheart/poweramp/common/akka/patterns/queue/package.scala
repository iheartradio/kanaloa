package com.iheart.poweramp.common.akka.patterns

import akka.actor.ActorRef

package object queue {
  type QueueRef = ActorRef
  type QueueProcessorRef = ActorRef
  type WorkerRef = ActorRef
}
