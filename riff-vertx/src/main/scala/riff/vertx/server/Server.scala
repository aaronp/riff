package riff.vertx.server
import com.typesafe.scalalogging.StrictLogging
import io.vertx.core.Handler
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.core.Vertx
import io.vertx.scala.core.http.{HttpServer, HttpServerRequest, ServerWebSocket}
import io.vertx.scala.ext.web.{Router, RoutingContext}
import io.vertx.scala.ext.web.handler.StaticHandler
import monix.execution.Scheduler
import monix.reactive.Observable
import streaming.api.HostPort
import streaming.rest.{RestRequestContext, WebURI}

import scala.concurrent.duration.Duration
import scala.util.Try

/**
  * Contains functions for starting a vertx service
  */
object Server extends StrictLogging {

  type OnConnect = ServerEndpoint => Unit

  object LoggingHandler extends Handler[HttpServerRequest] {
    override def handle(event: HttpServerRequest): Unit = {
      logger.info(s"Received $event")
    }
  }

  def startSocketWithHandler(hostPort: HostPort, capacity: Int)(onConnect: OnConnect)(implicit timeout: Duration, scheduler: Scheduler, vertx: Vertx): ScalaVerticle = {
    val websocketHandler: ServerWebSocketHandler = ServerWebSocketHandler("general", capacity)(onConnect)
    start(hostPort, None, LoggingHandler, websocketHandler)
  }

  def startSocket(hostPort: HostPort, capacity: Int, staticPath: Option[String] = None)(
      onConnect: PartialFunction[String, OnConnect])(implicit timeout: Duration, scheduler: Scheduler, vertx: Vertx): ScalaVerticle = {
    val websocketHandler = RoutingSocketHandler(onConnect.andThen(ServerWebSocketHandler("general", capacity)))
    start(hostPort, staticPath, LoggingHandler, websocketHandler)
  }

  def start(hostPort: HostPort, staticPath: Option[String], requestHandler: Handler[HttpServerRequest], socketHandler: Handler[ServerWebSocket])(
      implicit vertxInst: Vertx): ScalaVerticle = {
    object Server extends ScalaVerticle {
      vertx = vertxInst

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

  def startRest(hostPort: HostPort, staticPath: Option[String])(implicit scheduler: Scheduler): (ScalaVerticle with AutoCloseable, Observable[RestRequestContext]) = {
    val restHandler = RestHandler()
    object RestVerticle extends ScalaVerticle with AutoCloseable {
      vertx = Vertx.vertx()

      val requestHandler: Handler[HttpServerRequest] = makeHandler(hostPort, vertx, restHandler, staticPath)
      private lazy val server: HttpServer = {
        vertx
          .createHttpServer()
          .requestHandler(requestHandler)
          .listen(hostPort.port, hostPort.host)
      }
      override def start(): Unit = {
        server
      }
      override def close(): Unit = {
        stop()
        Try(server.close())
        Try(vertx.close())
      }
    }
    RestVerticle.start()

    RestVerticle -> restHandler.requests
  }

  private def makeHandler(vertx: Vertx, restHandlerByUri: Seq[(WebURI, Handler[RoutingContext])]): Handler[HttpServerRequest] = {

    val router = Router.router(vertx)

    import streaming.rest.HttpMethod._

    restHandlerByUri.foreach {
      case (uri @ WebURI(POST, _), handler) =>
        router.post(uri.pathString).handler(handler)
      case (uri @ WebURI(GET, _), handler) =>
        router.get(uri.pathString).handler(handler)
      case (uri @ WebURI(PUT, _), handler) =>
        router.put(uri.pathString).handler(handler)
      case (uri @ WebURI(DELETE, _), handler) =>
        router.delete(uri.pathString).handler(handler)
      case (uri @ WebURI(method, _), _) => sys.error(s"TODO: Unsupported/unimplemented handler method '${method}' : ${uri.pathString}")
    }

    router.accept _
  }

  def asStaticHandler(staticPath: String) = {
    val staticHandler: StaticHandler = StaticHandler.create().setDirectoryListing(true).setAllowRootFileSystemAccess(true).setWebRoot(staticPath)
    WebURI.get("/*") -> staticHandler
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
