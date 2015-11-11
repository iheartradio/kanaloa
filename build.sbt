organization in Global := "com.iheart"

name := "kanaloa"


lazy val root = (project in file(".")).configs(Testing.Integration)

scalacOptions ++= List("-feature", "-deprecation", "-unchecked", "-Xlint")

Dependencies.settings

Format.settings

Publish.settings

Testing.settings

