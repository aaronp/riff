package riff.web.vertx

import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.scalalogging.LazyLogging
import io.vertx.core.buffer.Buffer
import io.vertx.scala.core.http.WebSocketBase
import monix.execution.Ack
import monix.execution.Ack.Continue
import monix.reactive.Observer
import streaming.api.sockets._

import scala.concurrent.Future
import scala.util.control.NonFatal

private[vertx] final case class WebSocketObserver(name: String, socket: WebSocketBase) extends Observer[WebFrame] with LazyLogging {
  override def onNext(elem: WebFrame): Future[Ack] = elem match {
    case TextFrame(text) =>
      logger.debug(s"$name writing  to socket: $text")
      socket.writeTextMessage(text)
      Continue
    case BinaryFrame(data) =>
      logger.debug(s"$name writing bytes to socket")
      val buff: Buffer = io.vertx.core.buffer.Buffer.buffer(data.array)
      socket.writeBinaryMessage(buff)
      Continue
    case FinalTextFrame(text) =>
      logger.debug(s"$name writing final text to socket: $text")
      socket.writeFinalTextFrame(text)
      Continue
    case FinalBinaryFrame(data) =>
      logger.debug(s"$name writing final binary frame")
      val buff: Buffer = io.vertx.core.buffer.Buffer.buffer(data.array)
      socket.writeFinalBinaryFrame(buff)
      Continue
    case CloseFrame(statusCode, reason) =>
      logger.debug(s"$name writing close frame to socket w/ status $statusCode, reason $reason")
      socket.close(statusCode, reason)
      Continue
    //Stop
  }

  override def onError(ex: Throwable): Unit = {
    val ok = completed.compareAndSet(false, true)

    logger.debug(s"\n\t!!!! $name onError trying to close the socket will  ${if (ok) "succeed" else "fail"}")

    try {
      if (ok) {
        logger.debug(s"$name onError($ex) closing the socket")
        socket.close(500, Option(s"Error: $ex"))
      } else {
        logger.warn(s"onError($ex) has not effect on the closed socket")
      }
    } catch {
      case NonFatal(e) =>
        logger.error(s"Error ending socket connected to ${socket.remoteAddress()} after error $ex", e)
    }
  }

  private val completed = new AtomicBoolean(false)

  override def onComplete(): Unit = {

    val ok = completed.compareAndSet(false, true)
    logger.debug(s"\n\t!!!! $name onComplete trying to close the socket will  ${if (ok) "succeed" else "fail"}")

    try {
      if (ok) {
        socket.end()
      } else {
        logger.warn(s"$name onComplete has no effect as the socket it already closed")
      }
    } catch {
      case NonFatal(e) =>
        logger.error(s"$name Error ending socket connected to ${socket.remoteAddress()}", e)
    }
  }
}
