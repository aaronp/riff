package riff.web.vertx.server

import com.typesafe.scalalogging.StrictLogging
import io.vertx.core.Handler
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.core.Vertx
import io.vertx.scala.core.http.{HttpServerRequest, ServerWebSocket}
import io.vertx.scala.ext.web.Router
import io.vertx.scala.ext.web.handler.StaticHandler
import monix.execution.Scheduler
import monix.reactive.Observable
import streaming.api.HostPort
import streaming.rest.RestRequestContext

import scala.concurrent.duration.Duration

object Server extends StrictLogging {

  type OnConnect = ServerEndpoint => Unit

  object LoggingHandler extends Handler[HttpServerRequest] {
    override def handle(event: HttpServerRequest): Unit = {
      logger.info(s"Received $event")
    }
  }

  def startSocket(hostPort: HostPort)(onConnect: OnConnect)(implicit timeout: Duration, scheduler: Scheduler): ScalaVerticle = {
    val websocketHandler = ServerWebSocketHandler.replay("general")(onConnect)
    start(hostPort, None, LoggingHandler, websocketHandler)
  }

  def start(hostPort: HostPort,
            staticPath : Option[String] = None)(
      onConnect: PartialFunction[String, OnConnect])(implicit timeout: Duration, scheduler: Scheduler): ScalaVerticle = {
    val websocketHandler = RoutingSocketHandler(onConnect.andThen(ServerWebSocketHandler.replay("general")))
    start(hostPort, staticPath, LoggingHandler, websocketHandler)
  }


//  def start(hostPort: HostPort, requestHandler: Handler[HttpServerRequest] = LoggingHandler, staticPath : Option[String] = None, nullableName: String = null)(
//      onConnect: PartialFunction[String, OnConnect])(implicit timeout: Duration, scheduler: Scheduler): ScalaVerticle = {
//    val name             = Option(nullableName).getOrElse("general")
//    val websocketHandler = RoutingSocketHandler(onConnect.andThen(ServerWebSocketHandler.replay(name)))
//    start(hostPort, requestHandler, websocketHandler)
//  }

  def start(hostPort: HostPort,
            staticPath : Option[String],
            requestHandler: Handler[HttpServerRequest],
            socketHandler: Handler[ServerWebSocket]): ScalaVerticle = {
    object Server extends ScalaVerticle {
      vertx = Vertx.vertx()

      override def start(): Unit = {
        vertx
          .createHttpServer()
          .requestHandler(requestHandler)
          .websocketHandler(socketHandler)
          .listen(hostPort.port, hostPort.host)
      }
    }
    Server.start()
    Server
  }

  def startRest(hostPort: HostPort, staticPath: Option[String])(implicit scheduler: Scheduler): Observable[RestRequestContext] = {
    val restHandler = RestHandler()
    object RestVerticle extends ScalaVerticle {
      vertx = Vertx.vertx()

      val requestHandler: Handler[HttpServerRequest] = makeHandler(hostPort, vertx, restHandler, staticPath)
      override def start(): Unit = {
        vertx
          .createHttpServer()
          .requestHandler(requestHandler)
          .listen(hostPort.port, hostPort.host)
      }
    }
    RestVerticle.start()

    restHandler.requests
  }

  private def makeHandler(hostPort: HostPort, vertx: Vertx, restHandler: Handler[HttpServerRequest], staticPath: Option[String]): Handler[HttpServerRequest] = {

    staticPath match {
      case Some(path) =>
        val router = Router.router(vertx)
        router.route("/rest/*").handler(ctxt => restHandler.handle(ctxt.request()))

        val staticHandler = StaticHandler.create().setDirectoryListing(true).setAllowRootFileSystemAccess(true).setWebRoot(path)
        router.route("/*").handler(staticHandler)

        logger.info(s"Starting REST server at $hostPort, serving static data under $path")
        router.accept _
      case None =>
        logger.info(s"Starting REST server at $hostPort")
        restHandler.handle _
    }
  }
}
