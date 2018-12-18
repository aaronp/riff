package riff.vertx
import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.scalalogging.StrictLogging
import io.vertx.core.Handler
import io.vertx.scala.core.http.{WebSocketBase, WebSocketFrame}
import monix.execution.Scheduler
import monix.reactive.{Observable, Pipe}
import streaming.api.sockets.WebFrame

import scala.concurrent.duration.Duration

object WebFrameEndpoint extends StrictLogging {

  def apply(name: String, socket: WebSocketBase, capacity: Int)(implicit timeout: Duration, scheduler: Scheduler): (WebSocketObserver, Observable[WebFrame]) = {

    val (frameSink, frameSource: Observable[WebFrame]) = {
      if (capacity <= 0) {
        Pipe.publish[WebFrame].unicast
      } else {
        Pipe.replayLimited[WebFrame](capacity).unicast
      }
    }

    val observable = WebSocketObserver(name, socket)

    val completed = new AtomicBoolean(false)

    def markComplete() = {
      if (completed.compareAndSet(false, true)) {
        frameSink.onComplete()
        //observable.onComplete()
      } else {
        logger.warn("frame sink already completed")
      }
    }

    socket.frameHandler(new Handler[WebSocketFrame] {
      override def handle(event: WebSocketFrame): Unit = {
        if (event.isClose()) {
          logger.info(s"\n\t$name handling close frame\n")
          markComplete()
        } else {
          val frame = WebSocketFrameAsWebFrame(event)
          logger.debug(s"\n\t$name handling frame ${frame}\n")
          frameSink.onNext(frame)
          // TODO - we should apply back-pressure, but also not block the event loop.
          // need to apply some thought here if this can work in the general case,
          // of if this should be made more explicit
          //Await.result(fut, timeout)
        }
      }
    })

    socket.exceptionHandler(new Handler[Throwable] {
      override def handle(event: Throwable): Unit = {
        logger.warn(s"\n\t$name got exception $event\n")
        frameSink.onError(event)
        //observable.onError(event)
        socket.close()
      }
    })
    socket.endHandler(new Handler[Unit] {
      override def handle(event: Unit): Unit = {
        logger.debug(s"\n\t$name ending!!!!!!!!!\n")
        markComplete()
      }
    })

    val source = frameSource
      .doOnComplete { () =>
        logger.info(s"\n>>> $name onComplete called\n")

      }
      .doOnError { err =>
        logger.info(s"\n>>> $name onError($err) called\n")
      }
      .doOnNext { x =>
        logger.info(s"\n>>> $name onNext($x) called\n")
      }

    (observable, source)
  }
}
