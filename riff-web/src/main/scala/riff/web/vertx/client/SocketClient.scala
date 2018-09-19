package riff.web.vertx.client

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.StrictLogging
import io.vertx.core.Handler
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.core.Vertx
import io.vertx.scala.core.http.{HttpClient, WebSocket}
import monix.execution.Scheduler
import streaming.api.Endpoint
import streaming.api.sockets.WebFrame
import streaming.rest.EndpointCoords
import streaming.vertx.WebFrameEndpoint

import scala.concurrent.duration.Duration

class SocketClient private(coords: EndpointCoords, client: Handler[WebSocket], impl: Vertx = Vertx.vertx()) extends ScalaVerticle {
  vertx = impl

  val httpsClient: HttpClient = vertx.createHttpClient.websocket(coords.port, host = coords.host, coords.resolvedUri, client)

  start()
}

object SocketClient {

  def connect(coords: EndpointCoords, name: String = null)(onConnect: Endpoint[WebFrame, WebFrame] => Unit)(implicit timeout: Duration, scheduler: Scheduler): SocketClient = {
    val counter = new AtomicInteger(0)
    val handler = new Handler[WebSocket] with StrictLogging {
      override def handle(event: WebSocket): Unit = {
        val nonNullName = Option(name).getOrElse(s"SocketClient to $coords") + s"#${counter.incrementAndGet()}"
        logger.info(s"$nonNullName connected to socket")
        val (fromRemote, toRemote) = WebFrameEndpoint.replay(nonNullName, event)
        onConnect(Endpoint(fromRemote, toRemote))
      }
    }

    new SocketClient(coords, handler)

  }
}
