import sbt.Keys._
import sbt._

object Dependencies {

  object Versions {
    val akka = "2.4.8"
  }

  val akka = Seq(
    "com.typesafe.akka" %% "akka-actor" % Versions.akka,
    "com.typesafe.akka" %% "akka-testkit" % Versions.akka % "test",
    "com.typesafe.akka" %% "akka-multi-node-testkit" % Versions.akka % "test",
    "com.typesafe.akka" %% "akka-slf4j" % Versions.akka
  )

  val akkaHttp = Seq(
    "com.typesafe.akka" %% "akka-http" % "2.0.4"
  )

  val akkaCluster = Seq(
    "com.typesafe.akka" %% "akka-cluster" % Versions.akka,
    "com.typesafe.akka" %% "akka-cluster-tools" % Versions.akka
  )
  val (test, integration) = {
    val specs = Seq(
      "org.scalatest" %% "scalatest" % "2.2.6",
      "org.mockito" % "mockito-core" % "1.10.19"
    )

    (specs.map(_ % "test"), specs.map(_ % "integration"))
  }

  val config = Seq(
    "com.iheart" %% "ficus" % "1.2.6"
  )

  lazy val commonSettings = Seq(
    scalaVersion in Global := "2.11.8",

    resolvers ++= Seq(
      Resolver.typesafeRepo("releases"),
      Resolver.jcenterRepo,
      Resolver.bintrayRepo("scalaz", "releases"),
      "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/"
    )
  )

  lazy val stressHttpFrontend = Seq(
    libraryDependencies ++= akkaHttp
  )

  lazy val settings = commonSettings ++ Seq(

    libraryDependencies ++= Dependencies.akka ++
      Dependencies.test ++
      Dependencies.config

  )

}

