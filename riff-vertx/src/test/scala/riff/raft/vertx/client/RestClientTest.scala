package riff.raft.vertx.client

import java.util.concurrent.atomic.AtomicInteger

import io.vertx.scala.core.Vertx
import io.vertx.scala.ext.web.Router
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.concurrent.Eventually
import riff.RiffSpec
import riff.vertx.client.RestClient
import riff.vertx.server.{RestHandler, Server}
import riff.rest.{HostPort, _}

object RestClientTest {
// these tests run concurrent in SBT, so we need separate ports
  private val nextPort = new AtomicInteger(7100)
}

class RestClientTest extends RiffSpec with Eventually {

  "RestClient.send" should {
    "send and receive values" in {
      val port = RestClientTest.nextPort.incrementAndGet()

      val getIdAndNameURI: WebURI = WebURI.get("/rest/:id/:name")
      val putUserURI              = WebURI.put("/user/:id")

      val getIdHandler = {
        val handler = RestHandler()

        handler.requests.foreach { ctxt => //
          val params = getIdAndNameURI.unapply(ctxt.request.uri).get
          val id     = params("id")
          val name   = params("name")

          ctxt.completeWith(RestResponse.json(s"""{
                                                 | "msg" : "handled ${ctxt.request.uri}",
                                                 | "userId" : "${id}",
                                                 | "userName" : "${name}"
                                                 |}""".stripMargin))
        }

        handler
      }

      val putUserHandler = {
        val handler = RestHandler()

        handler.requests.foreach { ctxt => //
          val id: String = putUserURI.unapply(ctxt.request.uri).get("id")
          ctxt.completeWith(RestResponse.json(s"""{ "put" : "${id}" }"""))
        }

        handler
      }

      val server = {
        val vertx = Vertx.vertx()
        val restRoutes = {
          val routes = Seq(
            getIdAndNameURI -> getIdHandler,
            putUserURI      -> putUserHandler,
          )

          Server.makeHandler(Router.router(vertx), routes)
        }
        Server.start(HostPort.localhost(port), restRoutes, Server.LoggingSockerHandler)(vertx)
      }

      val client: RestClient = RestClient.connect(HostPort.localhost(port))

      try {

        // verify /reset/get/id/name route
        {
          val getUser                            = getIdAndNameURI.resolve("id" -> 123.toString, "name" -> "example").right.get
          val response: Observable[RestResponse] = client.send(RestInput(getUser))
          val reply: List[RestResponse]          = response.toListL.runSyncUnsafe(testTimeout)
          reply.size shouldBe 1
          reply.head.bodyAsString shouldBe """{
                                             | "msg" : "handled /rest/123/example",
                                             | "userId" : "123",
                                             | "userName" : "example"
                                             |}""".stripMargin
        }

        // verify PUT /reset/get/id/name route
        {
          val putUser                            = putUserURI.resolve("id" -> 456.toString).right.get
          val response: Observable[RestResponse] = client.send(RestInput(putUser))
          val List(reply)                        = response.toListL.runSyncUnsafe(testTimeout)
          reply.bodyAsString shouldBe """{ "put" : "456" }""".stripMargin
        }

      } finally {
        server.close()
        client.close()
      }
    }
  }

}
