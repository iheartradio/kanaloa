

val commonSettings = Seq(
  scalaVersion := "2.11.8",
  scalacOptions ++= Seq(
    "-encoding", "UTF-8",
    "-deprecation", // warning and location for usages of deprecated APIs
    "-feature", // warning and location for usages of features that should be imported explicitly
    "-unchecked", // additional warnings where generated code depends on assumptions
    "-Xlint", // recommended additional warnings
    "-Ywarn-adapted-args", // Warn if an argument list is modified to match the receiver
    "-Ywarn-inaccessible",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen"
  ),
  organization in Global := "com.iheart",
  name := "kanaloa"
)


val noPublishing = Seq(publish := (), publishLocal := (), publishArtifact := false)


lazy val root = project.in(file("."))
  .aggregate(core, cluster)
  .settings(moduleName := "kanaloa")
  .settings(Publish.settings:_*)
  .settings(Publish.extraReleaseStep:_*)

lazy val stressBackend = project.in(file("./stress/backend"))
  .aggregate(core, cluster)
  .dependsOn(core, cluster)
  .settings(moduleName := "kanaloa-stress-backend")
  .settings(noPublishing:_*)

lazy val stressFrontend = project.in(file("./stress/frontend"))
  .aggregate(stressBackend)
  .dependsOn(stressBackend)
  .settings(moduleName := "kanaloa-stress-frontend")
  .settings(noPublishing:_*)
  .settings(Dependencies.stressHttpFrontend:_*)


lazy val core = project
  .configs(Testing.Integration)
  .settings(moduleName := "kanaloa-core")
  .settings(commonSettings:_*)
  .settings(Dependencies.settings:_*)
  .settings(Format.settings:_*)
  .settings(Publish.settings:_*)
  .settings(Testing.settings:_*)

lazy val cluster = project
  .dependsOn(core)
  .aggregate(core)
  .settings(moduleName := "kanaloa-cluster")
  .configs(Testing.Integration)
  .settings(commonSettings:_*)
  .settings(Dependencies.settings:_*)
  .settings(Format.settings:_*)
  .settings(Publish.settings:_*)
  .settings(Testing.settings:_*)
  .settings(ClusterTests.settings:_*)
  .settings(
    libraryDependencies ++= Dependencies.akkaCluster
  ).configs(MultiJvm)


addCommandAlias("validate", ";root;clean;compile;test;integration:test;multi-jvm:test")
addCommandAlias("root", ";project root")
