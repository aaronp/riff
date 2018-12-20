import sbtcrossproject.CrossPlugin.autoImport.{CrossType, crossProject}

val repo = "riff"
name := repo

val username            = "aaronp"
val scalaEleven         = "2.11.11"
val scalaTwelve         = "2.12.7"
val defaultScalaVersion = scalaTwelve
val scalaVersions       = Seq(scalaEleven, scalaTwelve)
crossScalaVersions := scalaVersions
organization := s"com.github.${username}"
scalaVersion := defaultScalaVersion
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

// see https://github.com/sbt/sbt-ghpages
// this exposes the 'ghpagesPushSite' task
enablePlugins(GhpagesPlugin)
enablePlugins(GitVersioning)
//enablePlugins(PamfletPlugin)
enablePlugins(SiteScaladocPlugin)

// see http://scalameta.org/scalafmt/
scalafmtOnCompile in ThisBuild := true
scalafmtVersion in ThisBuild := "1.4.0"

// Define a `Configuration` for each project, as per http://www.scala-sbt.org/sbt-site/api-documentation.html
val Core      = config("riffCoreJVM")
val RiffMonix = config("riffMonix")
val RiffFs2   = config("riffFs2")
val RiffRest  = config("riffRest")
val RiffAkka  = config("riffAkka")
val RiffVertx = config("riffVertx")

git.remoteRepo := s"git@github.com:$username/$repo.git"
ghpagesNoJekyll := true

lazy val scaladocSiteProjects =
  List((riffCoreJVM, Core), (riffMonix, RiffMonix), (riffRest, RiffRest), (riffFs2, RiffFs2), (riffAkka, RiffAkka), (riffVertx, RiffVertx))

lazy val scaladocSiteSettings = scaladocSiteProjects.flatMap {
  case (project: Project, conf) =>
    SiteScaladocPlugin.scaladocSettings(
      conf,
      mappings in (Compile, packageDoc) in project,
      s"api/${project.id}"
    )
  case _ => Nil // ignore cross-projects
}

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
  crossScalaVersions := scalaVersions,
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
  (testOptions in Test) += (Tests
    .Argument(TestFrameworks.ScalaTest, "-h", s"target/scalatest-reports-${name.value}", "-oN"))
)

test in assembly := {}

publishMavenStyle := true

// val siteWithScaladocAlt = project.in(file("site/scaladoc-alternative"))

lazy val root = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .enablePlugins(SiteScaladocPlugin)
  .enablePlugins(ParadoxPlugin)
  .enablePlugins(ScalaUnidocPlugin)
  .aggregate(
    riffCoreJS,
    riffCoreJVM,
    riffJsonJVM,
    riffJsonJS,
    riffRuntime,
    riffMonix,
    riffRest,
    riffFs2,
    riffAkka,
    riffVertx
  )
  .settings(scaladocSiteSettings)
  .settings(
    // TODO - check
    paradoxProperties += ("project.url" -> "https://aaronp.github.io/riff/docs/current/"),
    paradoxTheme := Some(builtinParadoxTheme("generic")),
    siteSourceDirectory := target.value / "paradox" / "site" / "main",
    siteSubdirName in ScalaUnidoc := "api/latest",
    publish := {},
    publishLocal := {}
  )

lazy val riffCoreCrossProject = crossProject(JSPlatform, JVMPlatform)
  .crossType(CrossType.Full)
  .withoutSuffixFor(JVMPlatform)
  .enablePlugins(TestNGPlugin)
  .settings(
    name := "riff-core",
    libraryDependencies ++= List(
//      "org.reactivestreams" %%% "reactive-streams-scalajs" % "1.0.0",
      "com.lihaoyi"   %%% "scalatags" % "0.6.7",
      "org.scalatest" %%% "scalatest" % "3.0.0" % "test"
    )
  )
  .in(file("riff-core"))
  .jvmSettings(
    name := "riff-core-jvm",
    coverageMinimum := 85,
    coverageFailOnMinimum := true,
    libraryDependencies ++= List(
      "com.lihaoyi"                %% "sourcecode"          % "0.1.4", // % "test",
      "org.scala-js"               %% "scalajs-stubs"       % scalaJSVersion % "provided",
      "com.github.aaronp"          %% "eie"                 % "0.0.3",
      "org.reactivestreams"        % "reactive-streams"     % "1.0.2",
      "org.reactivestreams"        % "reactive-streams-tck" % "1.0.2" % "test",
      "com.typesafe.scala-logging" %% "scala-logging"       % "3.7.2" % "test",
      "ch.qos.logback"             % "logback-classic"      % "1.1.11" % "test"
    ),
    // put scaladocs under 'api/latest'
    siteSubdirName in SiteScaladoc := "api/latest"
  )
  .jsSettings(
    name := "riff-core-js"
    //libraryDependencies += "org.reactivestreams" %%% "reactive-streams-scalajs" % "1.0.0"
  )

lazy val riffCoreJVM = riffCoreCrossProject.jvm
lazy val riffCoreJS  = riffCoreCrossProject.js

val circeVersion = "0.10.0"

lazy val riffJsonCrossProject = crossProject(JSPlatform, JVMPlatform)
  .crossType(CrossType.Full)
  .withoutSuffixFor(JVMPlatform)
  .settings(
    name := "riff-json",
    libraryDependencies ++= List(
      "com.lihaoyi"   %%% "scalatags" % "0.6.7",
      "org.scalatest" %%% "scalatest" % "3.0.0" % "test"
    ) ++ (List(
      "io.circe" %%% "circe-core",
      "io.circe" %%% "circe-generic",
      "io.circe" %%% "circe-parser"
    ).map(_ % circeVersion))
  )
  .in(file("riff-json"))
  .jvmSettings(name := "riff-json-jvm", libraryDependencies ++= List("org.pegdown" % "pegdown" % "1.4.2" % "test"))
  .jvmSettings(commonSettings: _*)
  .jsSettings(
    name := "riff-json-js"
  )
  .dependsOn(riffCoreCrossProject % "compile->compile;test->test")

lazy val riffJsonJVM = riffJsonCrossProject.jvm
lazy val riffJsonJS  = riffJsonCrossProject.js

lazy val riffMonix = project
  .in(file("riff-monix"))
  .settings(commonSettings: _*)
  .settings(parallelExecution in Test := false)
  .settings(name := s"${repo}-monix", coverageMinimum := 80, coverageFailOnMinimum := true, libraryDependencies ++= Dependencies.RiffMonix)
  .dependsOn(riffCoreJVM % "compile->compile;test->test")

lazy val riffRest = project
  .in(file("riff-rest"))
  .dependsOn(riffMonix % "compile->compile;test->test")
  .dependsOn(riffCoreJVM % "compile->compile;test->test")
  .dependsOn(riffJsonJVM % "compile->compile;test->test")
  .settings(name := s"${repo}-rest")
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= Dependencies.RiffRest)

lazy val riffFs2 = project
  .in(file("riff-fs2"))
  .dependsOn(riffCoreJVM % "compile->compile;test->test")
  .dependsOn(riffJsonJVM % "compile->compile;test->test")
  .settings(name := s"${repo}-fs2")
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= Dependencies.RiffFs2)

lazy val riffAkka = project
  .in(file("riff-akka"))
  .dependsOn(riffCoreJVM % "compile->compile;test->test")
  .dependsOn(riffJsonJVM % "compile->compile;test->test")
  .settings(name := s"${repo}-akka")
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= Dependencies.RiffAkka)

lazy val riffVertx = project
  .in(file("riff-vertx"))
  .dependsOn(riffRest % "compile->compile;test->test")
  .settings(name := s"${repo}-vertx")
  .settings(commonSettings: _*)
  .settings(parallelExecution in Test := false)
  .settings(libraryDependencies ++= Dependencies.RiffVertx)

lazy val riffRuntime = project
  .in(file("riff-runtime"))
  .settings(name := s"${repo}-runtime")
  .settings(commonSettings: _*)
  .dependsOn(riffCoreJVM % "compile->compile;test->test")
  .dependsOn(riffJsonJVM % "compile->compile;test->test")
  .dependsOn(riffMonix % "compile->compile;test->test")
  .dependsOn(riffVertx % "compile->compile;test->test")
  .dependsOn(riffAkka % "compile->compile;test->test")
  .dependsOn(riffFs2 % "compile->compile;test->test")

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
