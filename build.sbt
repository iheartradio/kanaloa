organization in Global := "com.iheart"

name := "kanaloa"


scalaVersion in Global := Versions.scala

lazy val reactiveDispatcher = project in file(".")


resolvers ++= Dependencies.resolvers

libraryDependencies ++= Dependencies.akka ++
                        Dependencies.test ++
                        Dependencies.other ++
                        Dependencies.config


scalacOptions ++= List("-feature", "-deprecation", "-unchecked", "-Xlint")

Formatting.formatSettings

testOptions in Test += Tests.Argument(TestFrameworks.Specs2, "-xonly")


Publish.settings
