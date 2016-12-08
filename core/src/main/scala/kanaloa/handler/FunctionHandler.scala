package kanaloa.handler

import kanaloa.Result

import scala.concurrent.{ExecutionContext, Future}

/**
 * For simple async function without the error handling capabiliy
 */
class SimpleFunctionHandler[TReq, TResp](f: TReq ⇒ Future[TResp])(implicit ex: ExecutionContext) extends Handler[TReq] {
  type Resp = TResp
  type Error = Nothing

  override def handle(req: TReq): Handling[Resp, Error] = new Handling[Resp, Error] {
    override val result: Future[Result[Resp, Error]] = f(req).map(r ⇒ Result(Right(r), None))
    override val cancellable: Option[Cancellable] = None
  }

  override def name: String = ???

}
