package kanaloa.queue

import akka.actor.{Terminated, ActorRef, Actor}
import kanaloa.queue.Sampler._

import scala.concurrent.duration._

private[kanaloa] trait Sampler extends Actor {

  val settings: SamplerSettings
  import settings._
  private var subscribers: Set[ActorRef] = Set.empty

  private val scheduledSampling = {
    import context.dispatcher
    context.system.scheduler.schedule(
      sampleInterval,
      sampleInterval,
      self,
      AddSample
    )
  }

  override def postStop(): Unit = {
    scheduledSampling.cancel()
    super.postStop()
  }

  protected def handleSubscriptions: Receive = {
    case Subscribe(s) ⇒
      subscribers += s
      context watch s
    case Unsubscribe(s) ⇒
      subscribers -= s
      context unwatch s
    case Terminated(s) ⇒
      subscribers -= s
  }

  protected def publish(s: Sample): Unit = {
    subscribers.foreach(_ ! s)
  }
}

object Sampler {

  trait Sample

  case object AddSample

  case class Subscribe(actorRef: ActorRef)
  case class Unsubscribe(actorRef: ActorRef)

  /**
   *
   * @param sampleInterval do one sampling each interval
   * @param minSampleDurationRatio minimum sample duration ratio to [[sampleInterval]]. Sample whose duration is less than this will be abandoned.
   */
  case class SamplerSettings(
    sampleInterval:         FiniteDuration = 1.second,
    minSampleDurationRatio: Double         = 0.3
  ) {
    val minSampleDuration: Duration = sampleInterval * minSampleDurationRatio
  }
}
