package riff.web.vertx.server

import io.vertx.core.Handler
import io.vertx.scala.core.http.ServerWebSocket
import monix.execution.Scheduler

import scala.concurrent.duration.Duration

private[server] class ServerWebSocketHandler private(name : String, onConnect: ServerEndpoint => Unit)(implicit timeout: Duration, scheduler: Scheduler)
    extends Handler[ServerWebSocket] {
  override def handle(socket: ServerWebSocket): Unit = {
    onConnect(ServerEndpoint.replay(socket, name))
  }
}

object ServerWebSocketHandler {
  def replay(name : String)(onConnect: ServerEndpoint => Unit)(implicit timeout: Duration, scheduler: Scheduler): ServerWebSocketHandler = {
    new ServerWebSocketHandler(name, onConnect)
  }
}
