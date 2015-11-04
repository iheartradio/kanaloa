import sbt._

object Dependencies {
  object Versions {
    val akka        = "2.4.0"
    val specs2      = "3.0"
  }

  val resolvers = Seq(
    Resolver.typesafeRepo("releases"),
    Resolver.bintrayRepo("scalaz", "releases"),
    "Sonatype OSS Releases"  at "http://oss.sonatype.org/content/repositories/releases/"
  )

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

  val config = Seq(
    "net.ceedubs" %% "ficus" % "1.1.2"
  )

}

