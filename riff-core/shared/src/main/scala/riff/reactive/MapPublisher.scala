package riff.reactive
import org.reactivestreams.{Publisher, Subscriber, Subscription}

class MapPublisher[A, B](underlying: Publisher[A], f: A => B) extends Publisher[B] with AutoCloseable {
  override def subscribe(wrapped: Subscriber[_ >: B]): Unit = {
    underlying.subscribe(new Subscriber[A] {
      override def onSubscribe(s: Subscription): Unit = wrapped.onSubscribe(s)
      override def onNext(t: A): Unit = {
        wrapped.onNext(f(t))
      }
      override def onError(t: Throwable): Unit = wrapped.onError(t)
      override def onComplete(): Unit = wrapped.onComplete()
    })
  }
  override def close(): Unit = {
    underlying match {
      case closable: AutoCloseable => closable.close()
      case _ =>
    }
  }
}

object MapPublisher {

  def apply[A, B](underlying: Publisher[A])(f: A => B): MapPublisher[A, B] = {
    new MapPublisher[A, B](underlying, f)
  }
}
