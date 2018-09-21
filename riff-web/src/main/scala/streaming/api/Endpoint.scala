package streaming.api

import monix.execution.{Cancelable, Scheduler}
import monix.reactive.{Observable, Observer, Pipe}
import streaming.api.sockets.WebFrame

/**
  * Represents a connection to another system, either local or remote.
  *
  * Just a specialised kind of Pipe really which has typed inputs/outputs.
  *
  * @param toRemote an observer which can be attached to an Observable of events which should be sent to the endpoint
  * @param fromRemote an observable of messages coming from the endpoint
  * @tparam FromRemote
  * @tparam ToRemote
  */
class Endpoint[FromRemote, ToRemote](val toRemote: Observer[ToRemote], val fromRemote: Observable[FromRemote]) {
  final def map[A](f: FromRemote => A): Endpoint[A, ToRemote] = new Endpoint[A, ToRemote](toRemote, fromRemote.map(f))
  final def contraMap[A](f: A => ToRemote): Endpoint[FromRemote, A] = {
    import streaming.api.implicits._
    new Endpoint[FromRemote, A](toRemote.contraMap(f), fromRemote)
  }

  def handleTextFramesWith(
      f: Observable[String] => Observable[String])(implicit scheduler: Scheduler, fromEv: FromRemote =:= WebFrame, toEv: ToRemote =:= WebFrame): Cancelable = {
    val fromText = fromRemote.map { from =>
      fromEv(from).asText.getOrElse(sys.error("Received non-text frame"))
    }
    val replies: Observable[String] = f(fromText)
    replies.map[WebFrame](WebFrame.text).subscribe(toRemote.asInstanceOf[Observer[WebFrame]])
  }

}

object Endpoint {

  def instance[From, To](implicit endpoint: Endpoint[From, To]): Endpoint[From, To] = endpoint

  def apply[From, To](toRemote: Observer[To], fromRemote: Observable[From]): Endpoint[From, To] = {
    new Endpoint[From, To](toRemote, fromRemote)
  }

  def replay[T](initial: Seq[T]): Endpoint[T, T] = {
    val (to, from) = Pipe.replay[T](initial).unicast
    Endpoint(to, from)
  }
}
