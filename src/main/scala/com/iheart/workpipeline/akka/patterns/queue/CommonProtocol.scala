package com.iheart.workpipeline.akka.patterns.queue

object CommonProtocol {

  case class WorkRejected(reason: String)
  case class WorkFailed(reason: String)
  case class WorkTimedOut(reason: String)

}
