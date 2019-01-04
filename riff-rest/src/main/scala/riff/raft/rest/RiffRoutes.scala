package riff.raft.vertx.server
import com.typesafe.scalalogging.StrictLogging
import io.circe.generic.auto._
import io.circe.syntax._
import monix.execution.{CancelableFuture, Scheduler}
import monix.reactive.Observable
import riff.monix.log.LogStatus
import riff.rest.{RestRequestContext, RestResponse, WebURI}

object RiffRoutes {

  object log extends StrictLogging {

    val status = WebURI.get("/rest/riff/log/status")

    def statusHandler(requests: Observable[RestRequestContext], status: Observable[LogStatus])(implicit s: Scheduler): CancelableFuture[Unit] = {
      requests.zipWithIndex.foreach {
        case (ctxt, getRequestCount) =>
          logger.info(s"get log status $getRequestCount")
          val headResponse: Observable[RestResponse] = status.headF.map { status =>
            val json = status.asJson
            logger.info(s"get log status $getRequestCount is ${json.spaces4}\n")
            RestResponse.json(json.noSpaces)
          }
          headResponse.subscribe(ctxt.response)
          ctxt
      }
    }

//    def routes(logStatus: Observable[LogStatus])(implicit s: Scheduler): RestRoutes = {
//      val GetStatus = {
//        val statusRestHandler = RestHandler()
//        val fut               = statusHandler(statusRestHandler.requests, logStatus)
//        log.status -> statusRestHandler
//      }
//
//      List(GetStatus)
//    }
  }

}
