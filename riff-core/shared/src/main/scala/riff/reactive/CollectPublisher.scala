package riff.reactive
import org.reactivestreams.{Publisher, Subscriber, Subscription}

/**
  * Simple implementation of a publisher which can filter its published elements
  *
  * @param underlying the wrapped publisher
  * @param predicate
  * @tparam A
  */
class CollectPublisher[-A, B](underlying: Publisher[A], func: PartialFunction[A, B]) extends Publisher[B] with AutoCloseable {
  override def subscribe(wrappedSubscriber: Subscriber[_ >: B]): Unit = {
    underlying.subscribe(new Subscriber[A]() { self =>
      var subscription: Subscription = null
      override def onSubscribe(s: Subscription): Unit = {
        subscription = s
        wrappedSubscriber.onSubscribe(subscription)
      }
      override def onComplete(): Unit = {
        wrappedSubscriber.onComplete()
      }
      override def onError(err: Throwable): Unit = {
        wrappedSubscriber.onError(err)
      }
      override def onNext(next: A): Unit = {
        if (func.isDefinedAt(next)) {
          wrappedSubscriber.onNext(func(next))
        } else if (subscription != null) {
          subscription.request(1)
        }
      }
    })
  }

  override def close(): Unit = {
    underlying match {
      case closable: AutoCloseable => closable.close()
      case _ =>
    }
  }
}

object CollectPublisher {

  def apply[A, B](underlying: Publisher[A])(func: PartialFunction[A, B]): CollectPublisher[A, B] = {
    new CollectPublisher[A, B](underlying, func)
  }
}
