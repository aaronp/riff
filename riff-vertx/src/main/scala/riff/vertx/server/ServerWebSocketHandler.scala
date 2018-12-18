package riff.vertx.server

import io.vertx.core.Handler
import io.vertx.scala.core.http.ServerWebSocket
import monix.execution.Scheduler

import scala.concurrent.duration.Duration

private[server] class ServerWebSocketHandler private (name: String, capacity: Int, onConnect: ServerEndpoint => Unit)(implicit timeout: Duration, scheduler: Scheduler)
    extends Handler[ServerWebSocket] {
  override def handle(socket: ServerWebSocket): Unit = {
    onConnect(ServerEndpoint(socket, capacity, name))
  }
}

object ServerWebSocketHandler {

  def apply(name: String, capacity: Int)(onConnect: ServerEndpoint => Unit)(implicit timeout: Duration, scheduler: Scheduler): ServerWebSocketHandler = {
    new ServerWebSocketHandler(name, capacity, onConnect)
  }
}
