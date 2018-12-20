import sbt._

object Dependencies {

  val config: ModuleID = "com.typesafe" % "config" % "1.3.0"

  //https://github.com/typesafehub/scala-logging
  val logging = List(
    "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
    "ch.qos.logback"                                % "logback-classic" % "1.1.11")

  val testDependencies = List(
    "org.scalactic" %% "scalactic" % "3.0.5"   % "test",
    "org.scalatest" %% "scalatest" % "3.0.5"   % "test",
    "org.pegdown"                  % "pegdown" % "1.6.0" % "test",
    "junit"                        % "junit"   % "4.12" % "test"
  )

  val monix = List("monix", "monix-execution", "monix-eval", "monix-reactive", "monix-tail").map { art =>
    "io.monix" %% art % "3.0.0-RC1"
  }

  val simulacrum: ModuleID = "com.github.mpilquist" %% "simulacrum" % "0.13.0"

  val http4s = List("http4s-blaze-server", "http4s-circe", "http4s-dsl").map { art => "org.http4s" %% art % "0.18.12"
  }

  val vertx = List(
    "io.vertx" %% "vertx-lang-scala" % "3.5.2",
    "io.vertx" %% "vertx-web-scala"  % "3.5.2"
  )

  val fs2 = List("co.fs2" %% "fs2-core" % "0.10.4")

  val RiffMonix: List[ModuleID] = monix ::: logging ::: testDependencies
  val RiffFs2: List[ModuleID] = fs2 ::: logging ::: testDependencies

  val RiffAkka: List[ModuleID] = {
    val streams = "com.typesafe.akka" %% "akka-stream"       % "2.5.17"
    val akkaCirce = "de.heikoseeberger" %% "akka-http-circe" % "1.19.0"
    val cors = "ch.megard" %% "akka-http-cors"               % "0.2.2"
    val akkaHttp = List("", "-core").map { suffix => "com.typesafe.akka" %% s"akka-http$suffix" % "10.1.5"
    } :+ cors :+ akkaCirce :+ ("com.typesafe.akka" %% "akka-http-testkit" % "10.1.5" % "test")

    val akka = List(
      "com.typesafe.akka" %% "akka-actor"   % "2.5.14",
      "com.typesafe.akka" %% "akka-testkit" % "2.5.14" % Test
    )
    streams +: akkaHttp ::: akka ::: logging ::: testDependencies
  }
  val RiffHttp4s: List[ModuleID] = http4s ::: logging ::: testDependencies
  val RiffVertx: List[ModuleID] = config :: monix ::: vertx ::: logging ::: testDependencies
  val RiffRest: List[ModuleID] = config :: monix ::: logging ::: testDependencies
}
