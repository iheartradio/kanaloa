import sbt._, Keys._
import bintray.BintrayKeys._
import sbtrelease.ReleasePlugin.autoImport._
import ReleaseTransformations._


object Publish {

  val bintraySettings = Seq(
    bintrayOrganization := Some("iheartradio"),
    bintrayPackageLabels := Seq("akka", "reactive")
  )

  val publishingSettings = Seq(
    publishMavenStyle := true,
    licenses := Seq("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.html")),
    homepage := Some(url("http://iheartradio.github.io/kanaloa")),
    scmInfo := Some(ScmInfo(url("https://github.com/iheartradio/kanaloa"),
      "git@github.com:iheartradio/kanaloa.git")),
    pomIncludeRepository := { _ => false },
    publishArtifact in Test := false
  )

  val updateReadmeVersion = ReleaseStep(action = s ⇒ {
    val contents = IO.read(file("README.md"))

    val p = Project.extract(s)
    import p.get

    val libDependencyPattern = "(\"" + get(organization) + "\"\\s+%+\\s+\"" + get(name) + "\"\\s+%\\s+\")[\\w\\.-]+(\")"

    val newContents = contents.replaceAll(libDependencyPattern, "$1" + get(version) + "$2")

    IO.write(file("README.md"), newContents)

    val vcs = get(releaseVcs).getOrElse(sys.error("Aborting release. Working directory is not a repository of a recognized VCS."))
    vcs.add(file("README.md").getAbsolutePath) !! s.log

    s
  })

  def insertBeforeIn(seq: Seq[ReleaseStep], before: ReleaseStep, step: ReleaseStep) = {
    val (beforeStep, rest) =
      seq.span(_ != before)

    (beforeStep :+ step) ++ rest
  }

  val extraReleaseStep = Seq(
    releaseProcess := insertBeforeIn(releaseProcess.value, commitReleaseVersion, updateReadmeVersion)
  )

  val settings = bintraySettings ++ publishingSettings
}
