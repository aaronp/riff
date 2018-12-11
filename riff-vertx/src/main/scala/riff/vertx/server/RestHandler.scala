package riff.vertx.server

import com.typesafe.scalalogging.StrictLogging
import eie.io.{FromBytes, ToBytes}
import io.vertx.core.Handler
import io.vertx.core.buffer.Buffer
import io.vertx.scala.core.http.{HttpServerRequest, HttpServerResponse}
import monix.execution.{Ack, Scheduler}
import monix.reactive.{Observable, Observer, Pipe}
import riff.vertx.server.RestHandler.ResponseObserver
import streaming.rest._

import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class RestHandler(feed: Observer[RestRequestContext], val requests: Observable[RestRequestContext]) extends Handler[HttpServerRequest] with StrictLogging {

  override def handle(request: HttpServerRequest): Unit = {
    logger.debug(s"Handling request for ${request.uri()}")
    request.bodyHandler { buffer =>
      HttpMethod.unapply(request.method().name()) match {
        case None =>
          logger.error(s"Request for ${request.uri()} had a weird method: ${request.method().name()}")
          val response = request.response()
          response.setStatusCode(500)
          response.end(s"Unrecognized  method '${request.method().name()}'")
        case Some(method) =>
          logger.info(s"Creating ctxt for $method:${request.uri()}")
          val restContext = asRestContext(method, request, buffer)
          feed.onNext(restContext)
          request.resume()
      }
    }
  }

  private def asRestContext(method: HttpMethod, request: HttpServerRequest, buffer: Buffer) = {
    val body = buffer.getBytes

    logger.info(s"Handling ${request.method().name()} ${request.uri()}")
    val response: HttpServerResponse = request.response()

    val multiMap = request.headers()
    val headers: Map[String, String] = multiMap
      .names()
      .map { name =>
        (name, multiMap.getAll(name).mkString(","))
      }
      .toMap
    val restRequest = RestRequest(method, request.uri, body, headers)

    RestRequestContext(restRequest, new ResponseObserver(request.uri(), response))
  }

}

object RestHandler extends StrictLogging {

  private class ResponseObserver(uri: String, response: HttpServerResponse) extends Observer[RestResponse] with StrictLogging {
    var responseEnded = false

    override def onNext(elem: RestResponse): Future[Ack] = {
      try {
        respond(elem)
      } catch {
        case NonFatal(err) =>
          logger.error("Error processing response: $err")
          Ack.Stop
      }

    }
    private def respond(elem: RestResponse): Future[Ack] = {
      response.setStatusCode(elem.statusCode).setChunked(true)
      elem.headers.foreach {
        case (key, value) =>
          response.putHeader(key, value)
      }
      if (elem.body.nonEmpty) {
        response.write(io.vertx.core.buffer.Buffer.buffer(elem.body))
      }
      logger.info(s"Ending response to $uri")
      response.end()
      responseEnded = true
      Ack.Continue
    }

    override def onError(ex: Throwable): Unit = {
      logger.info(s"Ending response to $uri w/ error $ex")
      response.setStatusCode(500)
      response.end(s"Error handling ${uri} : $ex'")
      responseEnded = true
    }

    override def onComplete(): Unit = {
      logger.info(s"Ending response to $uri after onComplete, where responseEnded is $responseEnded")
      if (!responseEnded) {
        response.setStatusCode(200)
        response.end()
      }
    }
  }

  def asObservable[In: FromBytes, Out: ToBytes](req: HttpServerRequest)(handler: In => Out) = {

    //    val handler = onRequest(uri)

    req.handler { event: Buffer =>
      logger.debug("Handling content...")
      val body: Try[In] = FromBytes[In].read(event.getBytes)
      body.map(handler) match {
        case Success(reply) =>
          import ToBytes.ops._
          val data = Buffer.buffer(reply.bytes)
          req.response().setStatusCode(200)
          req.response().write(data)
        case Failure(err) =>
          req.response().setStatusCode(500)
          req.response().setStatusMessage(s"Error handling $body: $err")
      }
      logger.debug("ending response")
      req.response().end()
    }

  }

  def apply()(implicit scheduler: Scheduler): RestHandler = {

    val (contexts, observable: Observable[RestRequestContext]) = Pipe.publish[RestRequestContext].multicast

    new RestHandler(contexts, observable)
  }
}
