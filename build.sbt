organization in Global := "com.iheart"

name := "kanaloa"


scalaVersion in Global := "2.11.7"

resolvers ++= Dependencies.resolvers

libraryDependencies ++= Dependencies.akka ++
                        Dependencies.test ++
                        Dependencies.config


scalacOptions ++= List("-feature", "-deprecation", "-unchecked", "-Xlint")

Formatting.formatSettings

testOptions in Test += Tests.Argument(TestFrameworks.Specs2, "-xonly")


Publish.settings
