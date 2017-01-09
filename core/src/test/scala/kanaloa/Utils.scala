package kanaloa

import akka.actor.ActorDSL._
import akka.actor.{ActorRef, ActorRefFactory, ActorSystem}
import akka.testkit.TestProbe
import kanaloa.Sampler.SamplerSettings
import kanaloa.metrics.Reporter
import kanaloa.queue.QueueProcessor.WorkerPoolSamplerFactory
object Utils {

  class Factories(implicit system: ActorSystem) {
    def silent(implicit ac: ActorRefFactory): ActorRef = actor {
      new Act {
        become {
          case _ ⇒
        }
      }
    }

    def workPoolSampler(
      queueSampler: ActorRef         = TestProbe().ref,
      settings:     SamplerSettings  = SamplerSettings(),
      reporter:     Option[Reporter] = None
    ): WorkerPoolSamplerFactory = WorkerPoolSamplerFactory(queueSampler, settings, reporter)

    def workPoolSampler(
      returnRef: ActorRef
    ): WorkerPoolSamplerFactory = new WorkerPoolSamplerFactory {
      def apply()(implicit ac: ActorRefFactory): ActorRef = returnRef
    }

  }

}
