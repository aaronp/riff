package streaming.rest

import com.typesafe.scalalogging.LazyLogging
import monix.reactive.Observer

case class RestRequestContext(request: RestRequest, response: Observer[RestResponse]) extends LazyLogging {

  def completeWith(resp: RestResponse): Unit = {
    logger.debug(s"completing ${request.uri} with $resp")
    response.onNext(resp)
    response.onComplete()
  }
}

case class RestRequest(method: HttpMethod, uri: String, body: Array[Byte], headers: Map[String, String]) {
  def bodyAsString = new String(body)

  override def toString = s"${method} $uri ${headers.mkString("[", "; ", "]")} { $bodyAsString } "
}

case class RestResponse(body: Array[Byte], headers: Map[String, String], statusCode: Int = 200) {
  def bodyAsString = new String(body)
  override def toString = s"RestResponse(statusCode=$statusCode, headers=$headers, body=$bodyAsString)"
}

object RestResponse {
  def json(body: String): RestResponse = RestResponse(body.getBytes("UTF-8"), Map("content-type" -> "application/json"))
  def text(body: String): RestResponse = RestResponse(body.getBytes("UTF-8"), Map("content-type" -> "text/plain"))
}
