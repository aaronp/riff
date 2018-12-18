package riff.raft.vertx.server
import com.typesafe.scalalogging.StrictLogging
import io.circe.generic.auto._
import io.circe.syntax._
import monix.execution.Scheduler
import monix.reactive.Observable
import riff.monix.log.LogStatus
import riff.vertx.server.RestHandler
import riff.vertx.server.Server.RestRoutes
import streaming.rest.{RestRequestContext, RestResponse, WebURI}

object RiffRoutes {

  def routes(status: Observable[LogStatus])(implicit s: Scheduler): RestRoutes = {
    log.routes(status)
  }

  object log extends StrictLogging {

    val status = WebURI.get("/rest/riff/log/status")

    def statusHandler(requests: Observable[RestRequestContext], status: Observable[LogStatus])(implicit s: Scheduler) = {
      requests.zipWithIndex.foreach {
        case (ctxt, getRequestCount) =>
          logger.info(s"get log status $getRequestCount")
          val headResponse = status.headF.map { status =>
            val json = status.asJson
            logger.info(s"get log status $getRequestCount is ${json.spaces4}\n")
            RestResponse.json(json.noSpaces)
          }
          headResponse.subscribe(ctxt.response)
          ctxt
      }
    }

    def routes(logStatus: Observable[LogStatus])(implicit s: Scheduler): RestRoutes = {
      val GetStatus = {
        val statusRestHandler = RestHandler()
        val fut               = statusHandler(statusRestHandler.requests, logStatus)
        log.status -> statusRestHandler
      }

      List(GetStatus)
    }
  }

}
