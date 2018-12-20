package riff.vertx.client

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.StrictLogging
import io.vertx.core.Handler
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.core.Vertx
import io.vertx.scala.core.http.{HttpClient, WebSocket}
import monix.execution.Scheduler
import riff.vertx.WebFrameEndpoint
import riff.rest.sockets.WebFrame
import riff.rest.{Endpoint, EndpointCoords}

import scala.concurrent.duration.Duration

class SocketClient private (coords: EndpointCoords, client: Handler[WebSocket], impl: Vertx) extends ScalaVerticle with AutoCloseable {
  vertx = impl

  val httpsClient: HttpClient = vertx.createHttpClient.websocket(coords.port, host = coords.host, coords.resolvedUri, client)

  start()

  def close() = {
    stop()
    vertx.close()
  }
}

object SocketClient {

  /**
    * try to connect to the given endpoint
    * @param coords
    * @param name
    * @param onConnect
    * @param timeout
    * @param scheduler
    * @param vertx
    * @return
    */
  def connect(coords: EndpointCoords, capacity: Int, name: String = null)(
      onConnect: Endpoint[WebFrame, WebFrame] => Unit)(implicit timeout: Duration, scheduler: Scheduler, vertx: Vertx): SocketClient = {
    val counter = new AtomicInteger(0)
    val handler = new Handler[WebSocket] with StrictLogging {
      override def handle(event: WebSocket): Unit = {
        val nonNullName = Option(name).getOrElse(s"SocketClient to $coords") + s"#${counter.incrementAndGet()}"
        logger.info(s"$nonNullName connected to socket")
        val (toRemote, fromRemote) = WebFrameEndpoint(nonNullName, event, capacity)

        onConnect(Endpoint(toRemote, fromRemote))
      }
    }

    new SocketClient(coords, handler, vertx)

  }
}
