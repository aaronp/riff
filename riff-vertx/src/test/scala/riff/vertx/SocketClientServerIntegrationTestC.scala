package riff.vertx

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.typesafe.scalalogging.StrictLogging
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.core.Vertx
import monix.execution.Scheduler.Implicits.global
import org.scalatest.concurrent.Eventually
import riff.RiffSpec
import riff.vertx.client.SocketClient
import riff.vertx.server.{Server, ServerEndpoint}
import riff.web.vertx.SocketClientServerIntegrationTest
import streaming.api._
import streaming.api.sockets.WebFrame
import streaming.rest.EndpointCoords

import scala.concurrent.duration._

class SocketClientServerIntegrationTestC extends RiffSpec with Eventually with StrictLogging {
  override implicit def testTimeout: FiniteDuration = 8.seconds

  "Server.startSocket / SocketClient.connect" should {
    "connect to a server" in {
      val port = SocketClientServerIntegrationTest.nextPort.incrementAndGet

      implicit val vertx     = Vertx.vertx()
      val receivedFromServer = new CountDownLatch(1)
      var fromServer         = ""
      val receivedFromClient = new CountDownLatch(1)
      var fromClient         = ""

      // start the server
      val started: ScalaVerticle = Server.startSocketWithHandler(HostPort.localhost(port), capacity = 10) { endpoint: ServerEndpoint =>
        endpoint.toRemote.onNext(WebFrame.text(s"hello from the server at ${endpoint.socket.path}"))

        endpoint.fromRemote.foreach { msg: WebFrame =>
          msg.asText.foreach(fromClient = _)
          receivedFromClient.countDown()
        }
      }

      val c: SocketClient = SocketClient.connect(EndpointCoords.get(HostPort.localhost(port), "/some/path"), capacity = 10) { endpoint =>
        endpoint.fromRemote.foreach { msg =>
          msg.asText.foreach(fromServer = _)
          receivedFromServer.countDown()
        }
        endpoint.toRemote.onNext(WebFrame.text("from the client"))
      }
      try {
        withClue(s"server didn't start within $testTimeout") {
          receivedFromServer.await(testTimeout.toMillis, TimeUnit.MILLISECONDS) shouldBe true
          receivedFromClient.await(testTimeout.toMillis, TimeUnit.MILLISECONDS) shouldBe true
        }

        fromServer shouldBe "hello from the server at /some/path"
        fromClient shouldBe "from the client"
      } finally {
        c.close()
        started.stop()
        vertx.closeFuture().futureValue
      }
    }
  }
}
