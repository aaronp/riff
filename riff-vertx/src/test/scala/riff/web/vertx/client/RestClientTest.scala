package riff.web.vertx.client

import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.concurrent.Eventually
import riff.RiffSpec
import riff.vertx.client.RestClient
import riff.vertx.server.Server
import streaming.api.HostPort
import streaming.rest._

import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.Try
import scala.util.control.NonFatal

class RestClientTest extends RiffSpec with Eventually {

  override implicit def testTimeout: FiniteDuration = 30.seconds

  "RestClient.send" should {
    "send and receive values" in {
      val found = (7000 to 7010).find { port =>
        val Index = WebURI.get("/index.html")

        val (server, requests: Observable[RestRequestContext]) = Server.startRest(HostPort.localhost(port), None)

        requests.foreach { ctxt => //
          ctxt.completeWith(RestResponse.json(s"""{ "msg" : "handled ${ctxt.request.uri}" }"""))
        }
        val client: RestClient = RestClient.connect(HostPort.localhost(port))

        try {
          val response: Observable[RestResponse] = client.send(RestInput(Index))
          val reply: List[RestResponse]          = response.toListL.runSyncUnsafe(testTimeout)
          reply.size shouldBe 1

          reply.head.bodyAsString shouldBe "{ \"msg\" : \"handled index.html\" }"
          true
        } catch {
          case NonFatal(_) => false
        } finally {
          client.stop()
          Try(server.stopFuture().futureValue)
        }
      }
      found.nonEmpty shouldBe true
    }
  }

  "RestClient.sendPipe" should {
    "send and receive data" in {
      val Index = WebURI.get("/index.html")
      val Save  = WebURI.post("/save/:name")
      val Read  = WebURI.get("/get/name")

      val found = (8000 to 8010).find { port =>
        val (server, serverRequests: Observable[RestRequestContext]) = Server.startRest(HostPort.localhost(port), None)
        serverRequests.foreach { req => //
          req.completeWith(RestResponse.text(s"handled ${req.request.method} request for ${req.request.uri} w/ body '${req.request.bodyAsString}'"))
        }

        val client = RestClient.connect(HostPort.localhost(port))
        try {

          val requests = List(
            RestInput(Index),
            RestInput(Save, Map("name"    -> "david")),
            RestInput(Save, Map("invalid" -> "no name")),
            RestInput(Read)
          )
          val responses = Observable.fromIterable(requests).pipeThrough(client.sendPipe)
          var received  = List[RestResponse]()
          responses.foreach { resp: RestResponse =>
            received = resp :: received
          }

          eventually {
            received.size shouldBe requests.size - 1
          }
          received.map(_.bodyAsString) should contain theSameElementsInOrderAs List(
            "handled GET request for get/name w/ body ''",
            "handled POST request for save/david w/ body ''",
            "handled GET request for index.html w/ body ''"
          )
          true
        } catch {
          case NonFatal(_) => false
        } finally {
          client.stop()
          Try(server.stopFuture().futureValue)
        }
      }
      found.nonEmpty shouldBe true
    }
  }
}
