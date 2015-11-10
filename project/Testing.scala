import sbt._
import sbt.Keys._

object Testing {
  lazy val Integration = config("integration").extend(Test)


  lazy val settings = {
    // Separate integration/unit tests based on class name
    def isIntegrationTest(name: String): Boolean = name.endsWith("Integration")
    def isUnitTest(name: String): Boolean = !isIntegrationTest(name)

    Seq(
      libraryDependencies ++= Dependencies.test ++ Dependencies.integration,
      scalacOptions in Test ++= Seq("-Yrangepos"),
      testOptions in Test := Seq(
        Tests.Argument(TestFrameworks.Specs2, "-xonly"),
        Tests.Filter(isUnitTest)),
      testOptions in Integration := Seq(Tests.Filter(isIntegrationTest))
    ) ++ inConfig(Integration)(Defaults.testTasks)
  }

}
