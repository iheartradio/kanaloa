organization in Global := "com.iheart"

name := "kanaloa"


lazy val root = (project in file(".")).configs(Testing.Integration)

scalacOptions ++= List("-feature", "-deprecation", "-unchecked", "-Xlint")

logLevel in update := Level.Warn

Dependencies.settings

Format.settings

Publish.settings

Testing.settings

