organization in Global := "com.iheart"

name := "kanaloa"

lazy val root = (project in file(".")).configs(Testing.Integration)

scalacOptions ++= List("-feature", "-deprecation", "-unchecked", "-Xlint")

Dependencies.settings

Format.settings

Publish.settings

Testing.settings

parallelExecution in Test := false
parallelExecution in Testing.Integration := false
//concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)
logBuffered in Test := true
logBuffered in Testing.Integration := true
testOptions in Test += Tests.Argument("sequential")
testOptions in Testing.Integration += Tests.Argument("sequential")

