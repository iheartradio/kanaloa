resolvers += "Typesafe Repository" at "https://repo.typesafe.com/typesafe/releases/"

resolvers += "Sonatype OSS Releases" at "https://oss.sonatype.org/service/local/staging/deploy/maven2"

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.6.0")

addSbtPlugin("me.lessis" % "bintray-sbt" % "0.3.0")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.1")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.3.5")

addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.1.0")

addSbtPlugin("com.typesafe.sbt" % "sbt-multi-jvm" % "0.3.11")

addSbtPlugin("io.gatling" % "gatling-sbt" % "2.2.0")

addSbtPlugin("com.fortysevendeg"  % "sbt-microsites" % "0.4.0")

addSbtPlugin("com.eed3si9n"        % "sbt-unidoc"            % "0.3.3")
