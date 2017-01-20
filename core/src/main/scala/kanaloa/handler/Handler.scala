package kanaloa.handler

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

trait Handler[-TReq] {
  type Resp
  type Error

  /**
   * Unique name of the handler within a handlerProvider
   */
  def name: String

  def handle(req: TReq): Handling[Resp, Error]
}

/**
 * Represent an handling of a request
 * The main purpose of this type is to provide the optional cancellable
 */
trait Handling[TResp, TError] {
  def result: Future[Result[TResp, TError]]
  def cancellable: Option[Cancellable]
}

trait Cancellable {
  def cancel(): Boolean
}

/**
 * @param reply the message replying to the client
 * @param instruction instruction to kanaloa
 */
case class Result[TResp, TError](
  reply:       Either[TError, TResp],
  instruction: Option[Instruction]
)

/**
 * Instruction from Handler to kanaloa
 */
sealed abstract class Instruction extends Product with Serializable

/**
 * Kanaloa should stop using this handler and abandon it.
 */
case object Terminate extends Instruction

/**
 * Kanaloa should pause using this handler for {{@param duration}}
 */
case class Hold(duration: FiniteDuration) extends Instruction

object Handler {
  type Aux[TReq, TResp, TError] = Handler[TReq] { type Resp = TResp; type Error = TError }
}
