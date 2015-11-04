organization in ThisBuild := "com.iheart"

name := "kanaloa"

version in ThisBuild := "0.1.1" + Versions.releaseType

scalaVersion in ThisBuild := Versions.scala


licenses += ("Apache-2.0", url("http://www.apache.org/licenses/"))

lazy val reactiveDispatcher = project in file(".")

bintrayOrganization := Some("iheartradio")

bintrayPackageLabels := Seq("akka", "reactive")

resolvers ++= Dependencies.resolvers

libraryDependencies ++= Dependencies.akka ++
                        Dependencies.test ++
                        Dependencies.other ++
                        Dependencies.config


scalacOptions ++= List("-feature", "-deprecation", "-unchecked", "-Xlint")

Formatting.formatSettings

testOptions in Test += Tests.Argument(TestFrameworks.Specs2, "-xonly")
