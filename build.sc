import mill._, scalalib._, publish._, scalafmt._, scalajslib._

// http://www.lihaoyi.com/mill/index.html

object const {
  def ScalaEleven = "2.11.11"
  def ScalaTwelve = "2.12.6"
}

/**
  * common module for all the Riff modules.
  */
trait BaseModule extends CrossSbtModule with PublishModule with ScalafmtModule {

  override def scalacOptions = Seq(
    "-deprecation",                      // Emit warning and location for usages of deprecated APIs.
    "-encoding", "utf-8",                // Specify character encoding used by source files.
    "-feature",                          // Emit warning and location for usages of features that should be imported explicitly.
    "-Xcheckinit",                       // Wrap field accessors to throw an exception on uninitialized access.
    //"-Xfatal-warnings",                  // Fail the compilation if there are any warnings.
    "-Ywarn-dead-code",                  // Warn when dead code is identified.
    "-Ywarn-extra-implicit",             // Warn when more than one implicit parameter section is defined.
    "-Ywarn-inaccessible",               // Warn about inaccessible types in method signatures.
    "-Ywarn-infer-any",                  // Warn when a type argument is inferred to be `Any`.
    "-Ywarn-nullary-override",           // Warn when non-nullary `def f()' overrides nullary `def f'.
    "-Ywarn-nullary-unit",               // Warn when nullary methods return Unit.
    "-Ywarn-numeric-widen",              // Warn when numerics are widened.
    "-Ywarn-unused:implicits",           // Warn if an implicit parameter is unused.
    "-Ywarn-unused:imports",             // Warn if an import selector is not referenced.
    "-Ywarn-unused:locals",              // Warn if a local definition is unused.
    "-Ywarn-unused:params",              // Warn if a value parameter is unused.
    "-Ywarn-unused:patvars",             // Warn if a variable bound in a pattern is unused.
    "-Ywarn-unused:privates"             // Warn if a private member is unused.
  )

  def testFrameworks = Seq("org.scalatest.tools.Framework")

  override val publishVersion = "0.0.1"

  override def pomSettings = PomSettings(
    description = "A Raft implementation",
    organization = "com.github.aaronp",
    url = "https://github.com/aaronp/riff",
    licenses = Seq(License.MIT),
    versionControl = VersionControl.github("aaronp", "riff"),
    developers = Seq(
      Developer("aaronp", "Aaron Pritzlaff", "https://github.com/aaronp")
    )
  )

  /**
    * supports:
    *
    * shared/src/main
    *
    * plus
    *
    * {{{
    *     js/src/main
    *    jvm/src/main
    * native/src/main
    * }}}
    * @return
    */
  def sources = T.sources(
    millSourcePath / platformSegment / "main" / "scala",
    millSourcePath / "shared" / "main" / "scala"
  )
  def platformSegment: String
}

trait BaseTestModule extends TestModule with ScalaModule {

  def platformSegment: String

  def ivyDeps = Agg(
    ivy"org.scalatest::scalatest:3.0.4",
    ivy"org.scalactic::scalactic:3.0.4"
    //ivy"org.pegdown::pegdown:1.6.0",
  )

  def sources = T.sources(
    millSourcePath / platformSegment / "test" / "scala",
    millSourcePath / "shared" / "test" / "scala"
  )
  def testFrameworks = Seq("org.scalatest.tools.Framework")
}

/** == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
  * CORE MODULE
  * == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
  */
//"2.11.11",
object core extends Cross[CoreJvmModule](const.ScalaTwelve)

class CoreJvmModule(val crossScalaVersion: String) extends BaseModule{
  def platformSegment = "jvm"

  def ivyDeps = T{
    super.ivyDeps() ++ Seq(
      //ivy"com.typesafe:config:1.3.0",
      //ivy"com.typesafe.scala-logging::scala-logging:3.7.2;classifier=test",
      ivy"com.github.aaronp::eie:0.0.3",
      ivy"org.reactivestreams:reactive-streams:1.0.2"
    )
  }
  object test extends Tests with BaseTestModule {
    def platformSegment = "jvm"
    def millSourcePath = build.millSourcePath / "core"

  }
}

object coreJs extends Cross[CoreJsModule](const.ScalaTwelve)
class CoreJsModule(val crossScalaVersion: String) extends BaseModule with ScalaJSModule {
    def platformSegment = "js"
    def scalaJSVersion  = "0.6.24"
    def ivyDeps = T{
      super.ivyDeps() ++ Seq(
        ivy"org.scala-js:scalajs-dom_sjs0.6_2.12:0.9.6",
        ivy"com.lihaoyi::scalatags:0.6.5",
        ivy"org.reactivestreams:reactive-streams:1.0.2"
      )
    }
    // TODO - set -P:scalajs:mapSourceURI:???
    //def scalacOptions = ...

    def millSourcePath = build.millSourcePath / "core"

    object test extends Tests with BaseTestModule {
      def platformSegment = "js"
      def millSourcePath  = build.millSourcePath / "core"
    }
}


/** == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
  * MONIX MODULE
  * == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
  */
object `riff-monix` extends Cross[RiffMonixJvmModule](const.ScalaTwelve)

class RiffMonixJvmModule(val crossScalaVersion: String) extends BaseModule{
  def platformSegment = "jvm"

  def ivyDeps = T{
    val monix = List("monix", "monix-execution",  "monix-eval", "monix-reactive", "monix-tail").map { art =>
      ivy"io.monix::$art:3.0.0-RC1"
    }
    super.ivyDeps() ++ monix
  }
  object test extends Tests with BaseTestModule {
    def platformSegment = "jvm"
    def millSourcePath = build.millSourcePath / "riff-monix"
  }
}