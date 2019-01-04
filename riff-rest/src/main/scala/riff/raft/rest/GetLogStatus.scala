package riff.raft.rest
import com.typesafe.scalalogging.StrictLogging
import io.circe.generic.auto._
import io.circe.syntax._
import monix.eval.Task
import monix.execution.{CancelableFuture, Scheduler}
import monix.reactive.Observable
import riff.monix.log.LogStatus
import riff.rest.{RestRequestContext, RestResponse}

import scala.concurrent.Future

trait GetLogStatus[F[_]] {

  def getLogStatus() : F[LogStatus]
}

object GetLogStatus extends StrictLogging {

  case class obs(status: Observable[LogStatus])(implicit s: Scheduler) extends GetLogStatus[Observable] {
    override def getLogStatus() : Observable[LogStatus] = status.headF
  }
  case class task(status: Observable[LogStatus])(implicit s: Scheduler) extends GetLogStatus[Task] {
    override def getLogStatus() : Task[LogStatus] = status.headL
  }
  case class future(status: Observable[LogStatus])(implicit s: Scheduler) extends GetLogStatus[Future] {
    override def getLogStatus() : Future[LogStatus] = status.headL.runAsync
  }

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
}
