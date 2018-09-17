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

  val monix = List("monix", "monix-execution",  "monix-eval", "monix-reactive", "monix-tail").map { art =>
    "io.monix" %% art % "3.0.0-RC1"
  }

  val simulacrum: ModuleID = "com.github.mpilquist" %% "simulacrum" % "0.13.0"

  val reactiveStreams = "org.reactivestreams" % "reactive-streams" % "1.0.2"

  val eie = "com.github.aaronp" %% "eie" % "0.0.3"
  val http4s = List("http4s-blaze-server", "http4s-circe", "http4s-dsl").map { art =>
    "org.http4s"      %% art  % "0.18.12"
  }
  val akka = List(
    "com.typesafe.akka" %% "akka-actor" % "2.5.14",
    "com.typesafe.akka" %% "akka-testkit" % "2.5.14" % Test
  )

  val fs2 = List("co.fs2" %% "fs2-core" % "0.10.4")

  val RiffCore: List[ModuleID] = reactiveStreams :: eie :: logging ::: testDependencies
  val RiffMonix: List[ModuleID] = monix ::: logging ::: testDependencies
  val RiffFs2: List[ModuleID] = fs2 ::: logging ::: testDependencies
  val RiffAkka: List[ModuleID] = akka ::: logging ::: testDependencies
  val RiffHttp4s: List[ModuleID] = http4s ::: logging ::: testDependencies
  val RiffWeb: List[ModuleID] = http4s ::: logging ::: testDependencies
}
