package riff.vertx

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.typesafe.scalalogging.StrictLogging
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.core.Vertx
import monix.execution.Cancelable
import monix.execution.Scheduler.Implicits.global
import org.scalatest.concurrent.Eventually
import riff.RiffSpec
import riff.vertx.client.SocketClient
import riff.vertx.server.{Server, ServerEndpoint}
import riff.web.vertx.SocketClientServerIntegrationTest
import riff.api._
import riff.api.sockets.WebFrame
import riff.rest.{Endpoint, EndpointCoords}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.util.Try

class SocketClientServerIntegrationTestB extends RiffSpec with Eventually with StrictLogging {
  override implicit def testTimeout: FiniteDuration = 8.seconds

  "Server.startSocket / SocketClient.connect" should {

    "notify the server when the client completes" in {
      val port = SocketClientServerIntegrationTest.nextPort.incrementAndGet

      val messagesReceivedByTheServer = ListBuffer[String]()
      val messagesReceivedByTheClient = ListBuffer[String]()
      var serverReceivedOnComplete    = false

      implicit val vertx = Vertx.vertx()
      def chat(endpoint: Endpoint[WebFrame, WebFrame]): Cancelable = {
        endpoint.handleTextFramesWith { incomingMsgs =>
          val obs = "Hi - you're connected to an echo-bot" +: incomingMsgs.doOnNext(msg => messagesReceivedByTheServer += msg).map("echo: " + _)

          obs.doOnComplete { () =>
            logger.debug("from remote on complete")
            serverReceivedOnComplete = true
          }
        }
      }

      val started: ScalaVerticle = Server.startSocketWithHandler(HostPort.localhost(port), capacity = 10)(chat)
      var c: SocketClient        = null
      try {

        val gotFive = new CountDownLatch(5)
        c = SocketClient.connect(EndpointCoords.get(HostPort.localhost(port), "/some/path"), capacity = 10) { endpoint =>
          endpoint.toRemote.onNext(WebFrame.text("from client"))
          endpoint.fromRemote.zipWithIndex.foreach {
            case (frame, i) =>
              logger.debug(s"$i SocketClient got : " + frame)
              messagesReceivedByTheClient += frame.asText.getOrElse("non-text message received from server")
              gotFive.countDown()
              endpoint.toRemote.onNext(WebFrame.text(s"client sending: $i"))
              if (gotFive.getCount == 0) {
                logger.debug("completing client...")
                endpoint.toRemote.onComplete()
              }
          }
        }

        gotFive.await(testTimeout.toMillis, TimeUnit.MILLISECONDS) shouldBe true
        eventually {
          serverReceivedOnComplete shouldBe true
        }

        eventually {
          messagesReceivedByTheClient should contain inOrder ("Hi - you're connected to an echo-bot",
          "echo: from client",
          "echo: client sending: 0",
          "echo: client sending: 1",
          "echo: client sending: 2",
          "echo: client sending: 3")
        }

        eventually {
          messagesReceivedByTheServer should contain inOrder ("from client",
          "client sending: 0",
          "client sending: 1",
          "client sending: 2",
          "client sending: 3")
        }

      } finally {
        started.stop()
        if (c != null) {
          c.close()
        }
        vertx.closeFuture().futureValue
      }

    }

  }
}
