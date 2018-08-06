val repo = "riff"
name := repo

val username            = "aaronp"
val scalaEleven         = "2.11.8"
val scalaTwelve         = "2.12.6"
val defaultScalaVersion = scalaTwelve
crossScalaVersions := Seq(scalaTwelve)
organization := s"com.github.${username}"
scalaVersion := defaultScalaVersion
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

enablePlugins(GitVersioning)
enablePlugins(GhpagesPlugin)
enablePlugins(PamfletPlugin)
enablePlugins(SiteScaladocPlugin)

addCompilerPlugin("org.spire-math"  %% "kind-projector" % "0.9.3")
addCompilerPlugin("org.scalamacros" % "paradise"        % "2.1.0" cross CrossVersion.full)

// see http://scalameta.org/scalafmt/
scalafmtOnCompile in ThisBuild := true
scalafmtVersion in ThisBuild := "1.4.0"

// Define a `Configuration` for each project, as per http://www.scala-sbt.org/sbt-site/api-documentation.html
val Core       = config("core")
val RiffMonix  = config("riff-monix")
val RiffFs2    = config("riff-fs2")
val RiffAkka   = config("riff-akka")
val RiffHttp4s = config("riff-http4s")

// see https://github.com/sbt/sbt-ghpages
// this exposes the 'ghpagesPushSite' task
enablePlugins(GhpagesPlugin)
git.remoteRepo := s"git@github.com:$username/$repo.git"
ghpagesNoJekyll := true

enablePlugins(PamfletPlugin)
enablePlugins(SiteScaladocPlugin)

lazy val scaladocSiteProjects =
  List((core, Core), (riffMonix, RiffMonix), (riffHttp4s, RiffHttp4s), (riffFs2, RiffFs2), (riffAkka, RiffAkka))

lazy val scaladocSiteSettings = scaladocSiteProjects.flatMap {
  case (project, conf) =>
    SiteScaladocPlugin.scaladocSettings(
      conf,
      mappings in (Compile, packageDoc) in project,
      s"api/${project.id}"
    )
}

// val siteWithScaladocAlt = project.in(file("site/scaladoc-alternative"))
//   .settings(scaladocSiteSettings)

lazy val root = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .enablePlugins(SiteScaladocPlugin)
  .enablePlugins(PamfletPlugin)
  .enablePlugins(ScalaUnidocPlugin)
  .aggregate(
    core,
    riffMonix,
    riffFs2,
    riffAkka,
    riffHttp4s
  )
  .settings(
    sourceDirectory in Pamflet := sourceDirectory.value / "site",
    siteSubdirName in ScalaUnidoc := "api/latest",
    addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), siteSubdirName in ScalaUnidoc)
    //,gitRemoteRepo := "git@github.com:aaronp/agora.git"
  )
//settings(sourceDirectory in Pamflet := sourceDirectory.value / "site")

lazy val settings = scalafmtSettings

def additionalScalcSettings = List(
  "-deprecation", // Emit warning and location for usages of deprecated APIs.
  "-encoding",
  "utf-8", // Specify character encoding used by source files.
  "-unchecked",
  //  "-explaintypes", // Explain type errors in more detail.
  "-unchecked", // Enable additional warnings where generated code depends on assumptions.
  "-Xcheckinit", // Wrap field accessors to throw an exception on uninitialized access.
  "-Xfatal-warnings", // Fail the compilation if there are any warnings.
  "-Xfuture", // Turn on future language features.
  "-Xlint:adapted-args", // Warn if an argument list is modified to match the receiver.
  "-Xlint:by-name-right-associative", // By-name parameter of right associative operator.
  "-Xlint:delayedinit-select", // Selecting member of DelayedInit.
  "-Xlint:doc-detached", // A Scaladoc comment appears to be detached from its element.
  "-Xlint:inaccessible", // Warn about inaccessible types in method signatures.
  "-Xlint:infer-any", // Warn when a type argument is inferred to be `Any`.
  "-Xlint:missing-interpolator", // A string literal appears to be missing an interpolator id.
  "-Xlint:nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
  "-Xlint:nullary-unit", // Warn when nullary methods return Unit.
  "-Xlint:option-implicit", // Option.apply used implicit view.
  "-Xlint:package-object-classes", // Class or object defined in package object.
  "-Xlint:poly-implicit-overload", // Parameterized overloaded implicit methods are not visible as view bounds.
  "-Xlint:private-shadow", // A private field (or class parameter) shadows a superclass field.
  "-Xlint:stars-align", // Pattern sequence wildcard must align with sequence component.
  "-Xlint:type-parameter-shadow", // A local type parameter shadows a type already in scope.
  "-Xlint:unsound-match", // Pattern match may not be typesafe.
  "-Yno-adapted-args", // Do not adapt an argument list (either by inserting () or creating a tuple) to match the receiver.
  "-Ywarn-dead-code", // Warn when dead code is identified.
  "-Ywarn-inaccessible", // Warn about inaccessible types in method signatures.
  "-Ywarn-infer-any", // Warn when a type argument is inferred to be `Any`.
  "-Ywarn-nullary-override", // Warn when non-nullary `def f()' overrides nullary `def f'.
  "-Ywarn-nullary-unit",     // Warn when nullary methods return Unit.
  //  "-Ywarn-numeric-widen", // Warn when numerics are widened.
  "-Ywarn-value-discard" // Warn when non-Unit expression results are unused.
)

val baseScalacSettings = List(
  "-deprecation", // Emit warning and location for usages of deprecated APIs.
  "-encoding",
  "utf-8", // Specify character encoding used by source files.
  "-feature", // Emit warning and location for usages of features that should be imported explicitly.
  "-language:reflectiveCalls", // Allow reflective calls
  "-language:higherKinds", // Allow higher-kinded types
  "-language:implicitConversions", // Allow definition of implicit functions called views
  "-unchecked",
  "-language:reflectiveCalls", // Allow reflective calls
  "-language:higherKinds",         // Allow higher-kinded types
  "-language:implicitConversions", // Allow definition of implicit functions called views
  //"-Xlog-implicits",
  "-Xfuture" // Turn on future language features.
)

val scalacSettings = baseScalacSettings

val commonSettings: Seq[Def.Setting[_]] = Seq(
  //version := parentProject.settings.ver.value,
  organization := s"com.github.${username}",
  scalaVersion := defaultScalaVersion,
  resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  autoAPIMappings := true,
  exportJars := false,
  crossScalaVersions := Seq(scalaEleven, scalaTwelve),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-XX:MaxMetaspaceSize=1g"),
  scalacOptions ++= scalacSettings,
  buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
  buildInfoPackage := s"${repo}.build",
  assemblyMergeStrategy in assembly := {
    case str if str.contains("application.conf") => MergeStrategy.discard
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  },
  // see http://www.scalatest.org/user_guide/using_scalatest_with_sbt
  (testOptions in Test) += (Tests.Argument(TestFrameworks.ScalaTest, "-h", s"target/scalatest-reports-${name.value}", "-oN")),
  // put scaladocs under 'api/latest'
  sourceDirectory in Pamflet := sourceDirectory.value / "site",
  siteSubdirName in SiteScaladoc := "api/latest"
)

test in assembly := {}

publishMavenStyle := true

lazy val core = project
  .in(file("core"))
  //.dependsOn(io % "compile->compile;test->test", configProject % "compile->compile;test->test")
  .settings(name := s"${repo}")
  .settings(commonSettings: _*)
  .settings(addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full))
  .settings(libraryDependencies ++= Dependencies.RiffCore)

lazy val riffMonix = project
  .in(file("riff-monix"))
  .dependsOn(core % "compile->compile;test->test", core % "compile->compile;test->test")
  .settings(name := s"${repo}-monix")
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= Dependencies.RiffMonix)

lazy val riffFs2 = project
  .in(file("riff-fs2"))
  .dependsOn(core % "compile->compile;test->test", core % "compile->compile;test->test")
  .settings(name := s"${repo}-fs2")
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= Dependencies.RiffFs2)

lazy val riffAkka = project
  .in(file("riff-akka"))
  .dependsOn(core % "compile->compile;test->test", core % "compile->compile;test->test")
  .settings(name := s"${repo}-akka")
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= Dependencies.RiffAkka)

lazy val riffHttp4s = project
  .in(file("riff-http4s"))
  .dependsOn(core % "compile->compile;test->test", core % "compile->compile;test->test")
  .settings(name := s"${repo}-http4s")
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= Dependencies.RiffHttp4s)

// see https://leonard.io/blog/2017/01/an-in-depth-guide-to-deploying-to-maven-central/
pomIncludeRepository := (_ => false)

// To sync with Maven central, you need to supply the following information:
pomExtra in Global := {
  <url>https://github.com/${username}/${repo}
  </url>
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      </license>
    </licenses>
    <developers>
      <developer>
        <id>
          ${username}
        </id>
        <name>Aaron Pritzlaff</name>
        <url>https://github.com/${username}/${repo}
        </url>
      </developer>
    </developers>
}
