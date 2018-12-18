package riff.vertx.server
import com.typesafe.scalalogging.StrictLogging
import io.vertx.core.Handler
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.core.Vertx
import io.vertx.scala.core.http.{HttpServerRequest, ServerWebSocket}
import io.vertx.scala.ext.web.handler.StaticHandler
import io.vertx.scala.ext.web.{Router, RoutingContext}
import monix.execution.Scheduler
import streaming.api.HostPort
import streaming.rest.WebURI

import scala.concurrent.duration.Duration
import scala.util.Try

/**
  * Contains functions for starting a vertx service
  */
object Server extends StrictLogging {

  type OnConnect = ServerEndpoint => Unit

  type RestRoutes = Seq[(WebURI, Handler[RoutingContext])]

  object LoggingHandler extends Handler[RoutingContext] {
    override def handle(event: RoutingContext): Unit = {
      logger.info(s"Received $event")
      event.response().close()
    }
  }

  object LoggingSockerHandler extends Handler[ServerWebSocket] {
    override def handle(event: ServerWebSocket): Unit = {
      logger.info(s"Received socket event $event")
      event.close(500, Option("not supported"))
    }
  }

  def startSocketWithHandler(hostPort: HostPort, capacity: Int)(
      onConnect: OnConnect)(implicit timeout: Duration, scheduler: Scheduler, vertx: Vertx): ScalaVerticle with AutoCloseable = {
    val websocketHandler: ServerWebSocketHandler = ServerWebSocketHandler("general", capacity)(onConnect)

    val justLog = {
      val route = WebURI.get("/*") -> LoggingHandler
      Server.makeHandler(Router.router(vertx), route :: Nil)
    }
    start(hostPort, justLog, websocketHandler)
  }

  def startSocket(hostPort: HostPort, capacity: Int, routes: RestRoutes = Nil)(
      onConnect: PartialFunction[String, OnConnect])(implicit timeout: Duration, scheduler: Scheduler, vertx: Vertx): ScalaVerticle with AutoCloseable = {
    val websocketHandler = RoutingSocketHandler(onConnect.andThen(ServerWebSocketHandler("general", capacity)))

    val requestHandler = makeHandler(Router.router(vertx), routes)

    start(hostPort, requestHandler, websocketHandler)
  }

  def start(hostPort: HostPort, requestHandler: Handler[HttpServerRequest], socketHandler: Handler[ServerWebSocket])(
      implicit vertxInst: Vertx): ScalaVerticle with AutoCloseable = {

    object Server extends ScalaVerticle with AutoCloseable {
      vertx = vertxInst

      lazy val server = vertx
        .createHttpServer()
        .requestHandler(requestHandler)
        .websocketHandler(socketHandler)
        .listen(hostPort.port, hostPort.host)

      override def start(): Unit = {
        server
      }
      override def close(): Unit = {
        stop()
        Try(server.close())
        Try(vertx.close())
      }
    }
    Server.start()
    Server
  }

  def makeHandler(router: Router, routes: RestRoutes): Handler[HttpServerRequest] = {

    import streaming.rest.HttpMethod._

    routes.foreach {
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

  def asStaticHandler(staticPath: String): (WebURI, Handler[RoutingContext]) = {
    val staticHandler: StaticHandler = StaticHandler.create().setDirectoryListing(true).setAllowRootFileSystemAccess(true).setWebRoot(staticPath)
    WebURI.get("/*") -> staticHandler
  }

//
//  def startRest(hostPort: HostPort, staticPath: Option[String])(implicit scheduler: Scheduler): (ScalaVerticle with AutoCloseable, Observable[RestRequestContext]) = {
//    val restHandler = RestHandler()
//    object RestVerticle extends ScalaVerticle with AutoCloseable {
//      vertx = Vertx.vertx()
//
//      val requestHandler: Handler[HttpServerRequest] = makeHandler(hostPort, vertx, restHandler, staticPath)
//      private lazy val server: HttpServer = {
//        vertx
//          .createHttpServer()
//          .requestHandler(requestHandler)
//          .listen(hostPort.port, hostPort.host)
//      }
//      override def start(): Unit = {
//        server
//      }
//      override def close(): Unit = {
//        stop()
//        Try(server.close())
//        Try(vertx.close())
//      }
//    }
//    RestVerticle.start()
//
//    RestVerticle -> restHandler.requests
//  }
//
//  private def makeHandler(hostPort: HostPort, vertx: Vertx, restHandler: Handler[HttpServerRequest], staticPath: Option[String]): Handler[HttpServerRequest] = {
//
//    staticPath match {
//      case Some(path) =>
//        val router = Router.router(vertx)
//        router.route("/rest/*").handler(ctxt => restHandler.handle(ctxt.request()))
//
//        val staticHandler = StaticHandler.create().setDirectoryListing(true).setAllowRootFileSystemAccess(true).setWebRoot(path)
//        router.route("/*").handler(staticHandler)
//
//        logger.info(s"Starting REST server at $hostPort, serving static data under $path")
//        router.accept _
//      case None =>
//        logger.info(s"Starting REST server at $hostPort")
//        restHandler.handle _
//    }
//  }
}
