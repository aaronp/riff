package riff.web.vertx.client

import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.concurrent.Eventually
import riff.RiffSpec
import riff.web.vertx.server.Server
import streaming.api.HostPort
import streaming.rest._

import scala.concurrent.duration.{FiniteDuration, _}

class RestClientTest extends RiffSpec with Eventually {

  "RestClient.send" should {
    "send and receive shit" in {
      val Index                                    = WebURI.get("/index.html")
      val port                                     = 1234
      val requests: Observable[RestRequestContext] = Server.startRest(HostPort.localhost(port), None)

      requests.foreach { ctxt => ctxt.completeWith(RestResponse.json(s"""{ "msg" : "handled ${ctxt.request.uri}" }"""))
      }
      val client: RestClient = RestClient.connect(HostPort.localhost(port))

      try {
        val response: Observable[RestResponse] = client.send(RestInput(Index))
        val reply: List[RestResponse]          = response.toListL.runSyncUnsafe(testTimeout)
        reply.size shouldBe 1

        reply.head.bodyAsString shouldBe "{ \"msg\" : \"handled index.html\" }"

      } finally {
        client.stop()
      }
    }
  }

  "RestClient.sendPipe" should {
    "send and receive shit" in {
      val Index          = WebURI.get("/index.html")
      val Save           = WebURI.post("/save/:name")
      val Read = WebURI.get("/get/name")
      val port           = 8000
      val serverRequests = Server.startRest(HostPort.localhost(port), None)
      serverRequests.foreach { req =>
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
        var received = List[RestResponse]()
        responses.foreach { resp: RestResponse => received = resp :: received
        }

        eventually {
          received.size shouldBe requests.size - 1
        }
        received.map(_.bodyAsString) should contain theSameElementsInOrderAs List(
          "handled GET request for get/name w/ body ''",
          "handled POST request for save/david w/ body ''",
          "handled GET request for index.html w/ body ''"
        )

      } finally {
        client.stop()
      }
    }
  }

  override implicit def testTimeout: FiniteDuration = 800000.seconds
}
