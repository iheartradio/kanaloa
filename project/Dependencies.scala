package com.iheart.poweramp.common

import sbt._

object Dependencies {



  val resolvers = Seq(Resolver.typesafeRepo("releases"))

  val akka = Seq(
    "com.typesafe.akka" %% "akka-actor" % Versions.akka,
    "com.typesafe.akka" %% "akka-testkit" % Versions.akka % "test",
    "com.typesafe.akka" %% "akka-slf4j" % Versions.akka
  )

  val test = Seq(
    "org.specs2" %% "specs2-core" % Versions.specs2 % "test",
    "org.specs2" %% "specs2-mock" % Versions.specs2 % "test",
    "org.specs2" %% "specs2-scalacheck" % Versions.specs2 % "test"
  )

}
