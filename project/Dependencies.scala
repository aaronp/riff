import sbt._

object Dependencies {

  val config = "com.typesafe" % "config" % "1.3.0"

  //https://github.com/typesafehub/scala-logging
  val logging = List("com.typesafe.scala-logging" %% "scala-logging" % "3.7.2", "ch.qos.logback" % "logback-classic" % "1.1.11" % "test")

  val testDependencies = List(
    "org.scalactic" %% "scalactic" % "3.0.4" % "test",
    "org.scalatest" %% "scalatest" % "3.0.4" % "test",
    "org.pegdown" % "pegdown" % "1.6.0" % "test",
    "junit" % "junit" % "4.12" % "test"
  )

  val cats = List("cats-core", "cats-free").map { art =>
    "org.typelevel" %% art % "1.1.0"
  }

  val monix = List("monix", "monix-execution",  "monix-eval", "monix-reactive", "monix-tail").map { art =>
    "io.monix" %% art % "3.0.0-RC1"
  } ++ List(
    "org.atnos" %% "eff" % "5.2.0", // http://atnos-org.github.io/eff/org.atnos.site.Installation.html
    "org.atnos" %% "eff-monix" % "5.2.0"
  )

  val Riff = config :: monix ::: cats ::: logging ::: testDependencies
}
