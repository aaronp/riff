package riff.web.vertx.server

import io.vertx.core.Handler
import io.vertx.scala.core.http.ServerWebSocket
import RoutingSocketHandler.NotFoundHandler

case class RoutingSocketHandler(byRoute: PartialFunction[String, Handler[ServerWebSocket]], notFound: Handler[ServerWebSocket] = NotFoundHandler) extends Handler[ServerWebSocket] {
  override def handle(event: ServerWebSocket): Unit = {
    val path = event.uri()
    val h = if (byRoute.isDefinedAt(path)) {
      byRoute(path)
    } else {
      notFound
    }

    h.handle(event)
  }
}

object RoutingSocketHandler {

  object NotFoundHandler extends Handler[ServerWebSocket] {
    override def handle(event: ServerWebSocket): Unit = {
      event.reject(404)
    }
  }

}
