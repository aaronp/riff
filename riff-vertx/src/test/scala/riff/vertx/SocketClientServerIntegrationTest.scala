package riff.web.vertx

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.typesafe.scalalogging.StrictLogging
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.core.Vertx
import monix.execution.Cancelable
import monix.execution.Scheduler.Implicits.global
import org.scalatest.concurrent.Eventually
import riff.RiffSpec
import riff.rest.sockets.WebFrame
import riff.rest.{Endpoint, EndpointCoords, _}
import riff.vertx.client.SocketClient
import riff.vertx.server.{Server, ServerEndpoint}

import scala.collection.mutable.ListBuffer
import scala.util.Try

object SocketClientServerIntegrationTest {

  // these tests run concurrent in SBT, so we need separate ports
  val nextPort = new AtomicInteger(8050)
}

class SocketClientServerIntegrationTest extends RiffSpec with Eventually with StrictLogging {

  import SocketClientServerIntegrationTest._

  "Server.startSocket / SocketClient.connect" should {
    "route endpoints accordingly" in {
      val port = nextPort.incrementAndGet

      val UserIdR = "/user/(.*)".r

      val testLog = ListBuffer[String]()
      def logInTest(msg: String) = testLog.synchronized {
        testLog += msg
      }
      def logs: String = testLog.synchronized {
        testLog.mkString("\n")
      }

      implicit val vertx = Vertx.vertx()
      val started: ScalaVerticle = Server.startSocket(HostPort.localhost(port), capacity = 10) {
        case "/admin" =>
          logInTest("Server received /admin")

          endpt =>
            val frame = WebFrame.text("Thanks for connecting to admin")
            logInTest(s"Admin server sending $frame")
            endpt.fromRemote.foreach { slurp =>
              logInTest(s"Admin connection ignoring $slurp")
            }
            endpt.toRemote.onNext(frame)
            endpt.toRemote.onComplete()
        case UserIdR(user) =>
          logInTest(s"handleTextFramesWith ...")
          _.handleTextFramesWith { clientMsgs =>
            clientMsgs.map(s"$user : " + _)
          }
      }

      var clients: Seq[SocketClient] = Nil
      var admin: SocketClient        = null
      val receivedFromRemote         = new AtomicInteger(0)
      try {
        var adminResults: List[String] = Nil

        val connectedCount = new AtomicInteger(0)

        admin = SocketClient.connect(EndpointCoords.get(HostPort.localhost(port), "/admin"), capacity = 10, "test admin client") { endpoint =>
          connectedCount.incrementAndGet()
          val frame = WebFrame.text("already, go!")
          logInTest(s"toRemote sending to client $frame")
          endpoint.toRemote.onNext(frame)

          logInTest(s"toRemote onComplete")
          endpoint.toRemote.onComplete()

          def debug(f: WebFrame) = {
            logInTest(s"\t\t debug GOT: $f")
            receivedFromRemote.incrementAndGet
          }
          endpoint.fromRemote.doOnNext(debug).dump("ADMIN FROM REMOTE").toListL.runAsync.foreach { list =>
            adminResults = list.flatMap(_.asText)
          }
        }

        eventually {
          withClue(s"Having connected ${connectedCount.get} times and received ${receivedFromRemote.get}:\n${logs}\n") {
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
    "notify the server when the client completes" in {
      val port = nextPort.incrementAndGet

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
    "connect to a server" in {
      val port = nextPort.incrementAndGet

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
