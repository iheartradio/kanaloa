package kanaloa

import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import com.typesafe.config.{Config, ConfigFactory}

//adapted from akka cluster test code
trait ClusterConfig extends MultiNodeConfig {

  def clusterNode(roleName: String, extraClusterRoles: String*): RoleName = {
    val r =  role(roleName)
    val clusterRoles = (roleName +: extraClusterRoles).map(n => s""" "$n" """).mkString(",")

    nodeConfig(r)(ConfigFactory.parseString(s"""akka.cluster.roles =[$clusterRoles]"""))
    r
  }

  def clusterConfigWithFailureDetectorPuppet: Config =
    ConfigFactory.parseString("akka.cluster.failure-detector.implementation-class = akka.cluster.FailureDetectorPuppet").
      withFallback(clusterConfig)

  def clusterConfig(failureDetectorPuppet: Boolean): Config =
    if (failureDetectorPuppet) clusterConfigWithFailureDetectorPuppet else clusterConfig

  def clusterConfig: Config = ConfigFactory.parseString("""
    akka.actor.provider = akka.cluster.ClusterActorRefProvider
    akka.cluster {
      jmx.enabled                         = off
      gossip-interval                     = 200 ms
      leader-actions-interval             = 200 ms
      unreachable-nodes-reaper-interval   = 500 ms
      periodic-tasks-initial-delay        = 300 ms
      publish-stats-interval              = 0 s # always, when it happens
      failure-detector.heartbeat-interval = 500 ms
    }
    akka.loglevel = ERROR
    akka.log-dead-letters = off
    akka.log-dead-letters-during-shutdown = off
    akka.remote.log-remote-lifecycle-events = off
    akka.loggers = ["akka.testkit.TestEventListener"]
    akka.test {
      single-expect-default = 5 s
    }
                                                        """)

}
