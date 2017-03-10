
import sbtunidoc.Plugin.UnidocKeys._
import com.typesafe.sbt.SbtGhPages.GhPagesKeys._

lazy val docsMappingsAPIDir = settingKey[String]("Name of subdirectory in site target directory for api docs")

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
    "-Ywarn-numeric-widen",
    "-language:existentials"
  ),
  organization in Global := "com.iheart",
  name := "kanaloa",
  git.remoteRepo := "git@github.com:iheartradio/kanaloa.git",
  scmInfo := Some(ScmInfo(url("https://github.com/iheartradio/kanaloa"), "scm:" + git.remoteRepo.value))
)


lazy val docsSettings = Seq(
  docsMappingsAPIDir := "api",
  micrositeName := "Kanaloa",
  micrositeDescription := "Kanaloa - resiliency against traffic oversaturation",
  micrositeAuthor := "Kanaloa contributors",
  micrositeHighlightTheme := "atom-one-light",
  micrositeHomepage := "http://iheartradio.github.io/kanaloa",
  micrositeBaseUrl := "kanaloa",
  micrositeDocumentationUrl := "/" + micrositeBaseUrl.value + "/" + docsMappingsAPIDir.value,
  micrositeGithubOwner := "iheartradio",
  micrositeGithubRepo := "kanaloa",
  autoAPIMappings := true,
  addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), docsMappingsAPIDir),
  ghpagesNoJekyll := false,
  fork in tut := true,
  micrositePalette := Map(
    "brand-primary"   -> "#5B5988",
    "brand-secondary" -> "#292E53",
    "brand-tertiary"  -> "#222749",
    "gray-dark"       -> "#49494B",
    "gray"            -> "#7B7B7E",
    "gray-light"      -> "#E5E5E6",
    "gray-lighter"    -> "#F4F3F4",
    "white-color"     -> "#FFFFFF"),
  fork in (ScalaUnidoc, unidoc) := true,
  unidocProjectFilter in (ScalaUnidoc, unidoc) :=
    inProjects(core, cluster),
  scalacOptions in (ScalaUnidoc, unidoc) ++= Seq(
    "-Xfatal-warnings",
    "-doc-source-url", scmInfo.value.get.browseUrl + "/tree/master€{FILE_PATH}.scala",
    "-sourcepath", baseDirectory.in(LocalRootProject).value.getAbsolutePath,
    "-diagrams"
  ),

  includeFilter in makeSite := "*.html" | "*.css" | "*.png" | "*.jpg" | "*.gif" | "*.js" | "*.swf" | "*.yml" | "*.md"
)


val noPublishing = Seq(publish := (), publishLocal := (), publishArtifact := false)


lazy val root = project.in(file("."))
  .aggregate(core, cluster)
  .settings(moduleName := "kanaloa")
  .settings(noPublishing)


lazy val core = project
  .configs(Testing.Integration)
  .settings(moduleName := "kanaloa-core")
  .settings(commonSettings)
  .settings(Dependencies.settings)
  .settings(Format.settings)
  .settings(Publish.settings)
  .settings(Publish.extraReleaseStep)
  .settings(Testing.settings)

lazy val cluster = project
  .dependsOn(core)
  .aggregate(core)
  .settings(moduleName := "kanaloa-cluster")
  .configs(Testing.Integration)
  .settings(commonSettings)
  .settings(Dependencies.settings)
  .settings(Format.settings)
  .settings(Publish.settings)
  .settings(Testing.settings)
  .settings(ClusterTests.settings)
  .settings(
    libraryDependencies ++= Dependencies.akkaCluster
  ).configs(MultiJvm)

//following three modules are for benchmarks
lazy val stressBackend = project.in(file("./stress/backend"))
  .aggregate(core, cluster)
  .dependsOn(core, cluster)
  .settings(commonSettings)
  .settings(moduleName := "kanaloa-stress-backend")
  .settings(noPublishing)
  .settings(
    libraryDependencies ++= Dependencies.akkaThrottler ++ Dependencies.akkaHttp
  )

lazy val stressFrontend = project.in(file("./stress/frontend"))
  .aggregate(stressBackend)
  .dependsOn(stressBackend)
  .settings(moduleName := "kanaloa-stress-frontend")
  .settings(noPublishing)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Dependencies.akkaHttp
  )

lazy val stressGatling = project.in(file("./stress/gatling"))
  .aggregate(stressFrontend)
  .dependsOn(stressFrontend)
  .enablePlugins(GatlingPlugin)
  .settings(moduleName := "kanaloa-stress-gatling")
  .settings(noPublishing)
  .settings(commonSettings)
  .settings(
    resolvers += Resolver.sonatypeRepo("snapshots"),
    libraryDependencies ++= Dependencies.gatling
  )

lazy val docs = project
  .dependsOn(core, cluster)
  .settings(commonSettings)
  .settings(unidocSettings)
  .settings(tutScalacOptions ~= (_.filterNot(Set("-Ywarn-unused-import", "-Ywarn-dead-code"))))
  .settings(docsSettings)
  .settings(noPublishing)
  .enablePlugins(MicrositesPlugin)



addCommandAlias("root", ";project root")
addCommandAlias("stress", ";stressGatling/gatling:test-only kanaloa.stress.Kanaloa*")
addCommandAlias("validate", ";root;clean;compile;test;integration:test;docs/makeMicrosite")
addCommandAlias("root", ";project root")

