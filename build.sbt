
organization in ThisBuild := "com.iheart"

version in ThisBuild := "1.0.0-" + Versions.releaseType

scalaVersion in ThisBuild := Versions.scala

lazy val workPipeLine = project in file(".")

resolvers ++= Dependencies.resolvers

libraryDependencies ++= Dependencies.akka ++
                        Dependencies.test




