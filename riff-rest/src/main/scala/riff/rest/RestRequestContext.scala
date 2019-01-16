package riff.rest
import com.typesafe.scalalogging.LazyLogging
import monix.reactive.Observer

case class RestRequestContext(request: RestRequest, response: Observer[RestResponse]) extends LazyLogging {

  def completeWith(resp: RestResponse): Unit = {
    logger.debug(s"completing ${request.uri} with $resp")
    response.onNext(resp)
    response.onComplete()
  }
}
