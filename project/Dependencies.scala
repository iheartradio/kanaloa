import sbt.Keys._
import sbt._

object Dependencies {

  object Versions {
    val akka = "2.4.0"
    val specs2 = "3.6.6"
  }

  val akka = Seq(
    "com.typesafe.akka" %% "akka-actor" % Versions.akka,
    "com.typesafe.akka" %% "akka-testkit" % Versions.akka % "test",
    "com.typesafe.akka" %% "akka-slf4j" % Versions.akka
  )

  val (test, integration) = {
    val specs = Seq(
      "org.specs2" %% "specs2-core" % Versions.specs2,
      "org.specs2" %% "specs2-mock" % Versions.specs2,
      "org.specs2" %% "specs2-scalacheck" % Versions.specs2
    )

    (specs.map(_ % "test"), specs.map(_ % "integration"))
  }

  val config = Seq(
    "com.iheart" %% "ficus" % "1.2.1"
  )

  lazy val settings = Seq(

    scalaVersion in Global := "2.11.7",

    resolvers ++= Seq(
      Resolver.typesafeRepo("releases"),
      Resolver.bintrayRepo("scalaz", "releases"),
      Resolver.sonatypeRepo("releases")
    ),

    libraryDependencies ++= Dependencies.akka ++
      Dependencies.test ++
      Dependencies.config

  )

}

