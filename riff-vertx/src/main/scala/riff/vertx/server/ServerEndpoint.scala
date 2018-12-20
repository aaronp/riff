package riff.vertx.server

import com.typesafe.scalalogging.StrictLogging
import io.vertx.scala.core.http.ServerWebSocket
import monix.execution.Scheduler
import monix.reactive.{Observable, Observer}
import riff.vertx.WebFrameEndpoint
import riff.rest.sockets.WebFrame
import riff.rest.Endpoint

import scala.concurrent.duration.Duration

/**
  * A specialised endpoint which retains a reference to the socket to which is it connected,
  * which can be queried for e.g. the uri, query string, etc
  *
  * @param socket
  * @param from
  * @param to
  */
final class ServerEndpoint(val socket: ServerWebSocket, to: Observer[WebFrame], from: Observable[WebFrame]) extends Endpoint[WebFrame, WebFrame](to, from)

object ServerEndpoint extends StrictLogging {

  def apply(socket: ServerWebSocket, capacity: Int, name: String)(implicit timeout: Duration, scheduler: Scheduler): ServerEndpoint = {
    val addr = {
      val a   = socket.remoteAddress()
      val url = s"${a.host}:${a.port}/${a.path}"
      s"$name (socket connected to $url)"
    }
    logger.info(s"$addr Accepting connection")
    val (obs, frameSource) = WebFrameEndpoint(addr, socket, capacity)
    socket.accept()
    new ServerEndpoint(socket, obs, frameSource)
  }

}
