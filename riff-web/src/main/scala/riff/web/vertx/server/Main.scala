package riff.web.vertx.server
import com.typesafe.scalalogging.StrictLogging
import monix.execution.CancelableFuture
import monix.reactive.Observable
import streaming.rest.{RestRequestContext, RestResponse, StreamingConfig}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main extends StrictLogging {

  def main(a: Array[String]) = {
    val config = StreamingConfig.fromArgs(a)
    logger.info(config.summary())

    implicit val scheduler                      = config.computeScheduler

    Server.startRest(config.hostPort, config.staticPath)


    val started: Observable[RestRequestContext] = Server.startRest(config.hostPort, config.staticPath)

    val task: CancelableFuture[Unit] = started.foreach { ctxt => ctxt.completeWith(RestResponse.text(s"Handled ${ctxt.request}"))
    }
    Await.result(task, Duration.Inf)
    logger.info("Done")
  }
}
