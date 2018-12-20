package riff.vertx

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.StrictLogging
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.core.Vertx
import monix.execution.CancelableFuture
import monix.execution.Scheduler.Implicits.global
import org.scalatest.concurrent.Eventually
import riff.RiffSpec
import riff.vertx.client.SocketClient
import riff.vertx.server.Server
import riff.web.vertx.SocketClientServerIntegrationTest
import riff.api._
import riff.api.sockets.WebFrame
import riff.rest.EndpointCoords

import scala.concurrent.duration._
import scala.util.Try

class SocketClientServerIntegrationTestA extends RiffSpec with Eventually with StrictLogging {
  override implicit def testTimeout: FiniteDuration = 8.seconds

  "Server.startSocket / SocketClient.connect" should {
    "route endpoints accordingly" in {
      val port = SocketClientServerIntegrationTest.nextPort.incrementAndGet

      val UserIdR = "/user/(.*)".r

      implicit val vertx = Vertx.vertx()
      val started: ScalaVerticle = Server.startSocket(HostPort.localhost(port), capacity = 10) {
        case "/admin" =>
          endpt =>
            val frame = WebFrame.text("Thanks for connecting to admin")
            logger.info(s"\nAdmin server sending $frame\n\n")
            endpt.fromRemote.foreach { slurp =>
              logger.info(s"Admin connection ignoring $slurp")
            }
            endpt.toRemote.onNext(frame)

            logger.info(s"\nAdmin server waiting for the first msg fro the client\n\n")
            val gotIt: CancelableFuture[WebFrame] = endpt.fromRemote.headL.runAsync

            gotIt.foreach { received =>
              logger.info(s"\nAdmin server got $received, completing")
              endpt.toRemote.onComplete()
            }
        case UserIdR(user) =>
          logger.info(s"handleTextFramesWith ...")
          _.handleTextFramesWith { clientMsgs =>
            clientMsgs.map(s"$user : " + _)
          }
      }

      var clients: Seq[SocketClient] = Nil
      var admin: SocketClient        = null
      val receivedFromRemote         = new AtomicInteger(0)
      try {
        var adminResults: List[String] = Nil

        admin = SocketClient.connect(EndpointCoords.get(HostPort.localhost(port), "/admin"), capacity = 10, "test admin client") { endpoint =>
          val frame = WebFrame.text("already, go!")
          logger.info(s"toRemote sending to client $frame")
          endpoint.toRemote.onNext(frame)

          logger.info(s"toRemote onComplete")
          endpoint.toRemote.onComplete()

          def debug(f: WebFrame) = {
            logger.info(s"\t\t GOT: $f")
            receivedFromRemote.incrementAndGet
          }
          endpoint.fromRemote.doOnNext(debug).dump("ADMIN FROM REMOTE").toListL.runAsync.foreach { list =>
            adminResults = list.flatMap(_.asText)
          }
        }

        withClue(s"Having received ${receivedFromRemote.get}") {
          eventually {
            adminResults shouldBe List("Thanks for connecting to admin")
          }
        }

        val resultsByUser = new java.util.concurrent.ConcurrentHashMap[String, List[String]]()
        clients = Seq("Alice", "Bob", "Dave").zipWithIndex.map {
          case (user, i) =>
            SocketClient.connect(EndpointCoords.get(HostPort.localhost(port), s"/user/$user"), capacity = 10) { endpoint =>
              endpoint.toRemote.onNext(WebFrame.text(s"client $user ($i) sending message")).onComplete { _ =>
                endpoint.toRemote.onComplete()
              }
              endpoint.fromRemote.dump(s"!!!! $user from remote").toListL.runAsync.foreach { clientList =>
                resultsByUser.put(user, clientList.flatMap(_.asText))
              }
            }
        }
        eventually {
          resultsByUser.get("Alice") shouldBe List("Alice : client Alice (0) sending message")
        }
        eventually {
          resultsByUser.get("Bob") shouldBe List("Bob : client Bob (1) sending message")
        }
        eventually {
          resultsByUser.get("Dave") shouldBe List("Dave : client Dave (2) sending message")
        }
      } finally {
        Try(admin.close())
        clients.foreach(_.close())
        started.stop()
        vertx.closeFuture().futureValue
      }
    }

  }
}
