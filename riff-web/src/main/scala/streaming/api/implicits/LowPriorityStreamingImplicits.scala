package streaming.api.implicits

import monix.execution.Ack
import monix.execution.Ack.Stop
import monix.reactive.Observer
import streaming.api.implicits.LowPriorityStreamingImplicits.RichObserver

import scala.concurrent.Future
import scala.util.control.NonFatal

trait LowPriorityStreamingImplicits {

  implicit def asRichObserver[T](obs: Observer[T]) = new RichObserver[T](obs)

}

object LowPriorityStreamingImplicits {

  class RichObserver[T](val obs: Observer[T]) extends AnyVal {

    def contraMap[A](f: A => T): Observer[A] = new Observer[A] {
      override def onNext(elem: A): Future[Ack] = obs.onNext(f(elem))

      override def onError(ex: Throwable): Unit = obs.onError(ex)

      override def onComplete(): Unit = obs.onComplete()
    }

    def contraMapP[A](f: PartialFunction[A, T]): Observer[A] = contraMapUnsafe {
      case a if f.isDefinedAt(a) => f(a)
      case a                     => sys.error(s"contraMap is undefined for $a")
    }

    def contraMapUnsafe[A](f: A => T): Observer[A] = new Observer[A] {
      override def onNext(elem: A): Future[Ack] = {
        try {
          obs.onNext(f(elem))
        } catch {
          case NonFatal(err) =>
            onError(err)
            Stop
        }
      }

      override def onError(ex: Throwable): Unit = obs.onError(ex)

      override def onComplete(): Unit = obs.onComplete()
    }
  }

}
