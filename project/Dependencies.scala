import sbt.Keys._
import sbt._

object Dependencies {

  object Versions {
    val akka = "2.4.8"
  }

  val akka = Seq(
    "com.typesafe.akka" %% "akka-actor" % Versions.akka,
    "com.typesafe.akka" %% "akka-testkit" % Versions.akka % "test",
    "com.typesafe.akka" %% "akka-slf4j" % Versions.akka
  )

  val akkaCluster = Seq(
    "com.typesafe.akka" %% "akka-cluster" % Versions.akka,
    "com.typesafe.akka" %% "akka-cluster-tools" % Versions.akka
  )

  val akkaHttp = Seq(
    "com.typesafe.akka" %% "akka-http-experimental" % Versions.akka
  )

  val akkaThrottler = Seq(
    "com.typesafe.akka" %% "akka-contrib" % Versions.akka
  )

  val gatling = Seq(
    "io.gatling.highcharts" % "gatling-charts-highcharts" % "2.2.3-SNAPSHOT" % Test,
    "io.gatling"            % "gatling-test-framework"    % "2.2.3-SNAPSHOT" % Test
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


  lazy val settings = Seq(

    scalaVersion in Global := "2.11.8",

    resolvers ++= Seq(
      Resolver.typesafeRepo("releases"),
      Resolver.jcenterRepo,
      Resolver.bintrayRepo("scalaz", "releases"),
      Resolver.sonatypeRepo("releases")
    ),

    libraryDependencies ++= Dependencies.akka ++
      Dependencies.test ++
      Dependencies.config
  )


}

