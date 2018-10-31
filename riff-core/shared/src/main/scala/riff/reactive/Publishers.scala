package riff.reactive
import java.util.concurrent.atomic.AtomicBoolean

import org.reactivestreams.{Publisher, Subscriber, Subscription}

object Publishers {

  object NoOpSubscription extends Subscription {
    override def request(n: Long): Unit = {}
    override def cancel(): Unit = {}
  }

  final case class InError[T](exp: Throwable) extends Publisher[T] {
    override def subscribe(s: Subscriber[_ >: T]): Unit = {
      s.onSubscribe(NoOpSubscription)
      s.onError(exp)
    }
  }

  case object Completed extends Publisher[Nothing] {
    override def subscribe(s: Subscriber[_]): Unit = {
      s.onSubscribe(NoOpSubscription)
      s.onComplete()
    }
  }

  case class TakeWhile[A](underlying: Publisher[A], test: A => Boolean, includeLast : Boolean) extends Publisher[A] {
    override def subscribe(subscriber: Subscriber[_ >: A]): Unit = {
      underlying.subscribe(new Subscriber[A] {
        private var subscription: Subscription = null
        private var completed = false
        override def onSubscribe(s: Subscription): Unit = {
          val doubleSubscription = subscription != null
          subscription = s
          subscriber.onSubscribe(s)
          if (doubleSubscription) {
            s.cancel()
            subscriber.onError(new Exception("multiple subscriptions"))
          }
        }
        override def onNext(next: A): Unit = {
          if (test(next) && !completed) {
            subscriber.onNext(next)
          } else {
            val s = subscription
            if (s != null) {
              s.cancel()
              if (!completed) {
                if (includeLast) {
                  subscriber.onNext(next)
                }
                completed = true
                subscriber.onComplete()
              }
              subscription = null
            }
          }
        }
        override def onError(t: Throwable): Unit = {
          if (!completed) {
            completed = true
            subscriber.onError(t)
            subscription = null
          }
        }
        override def onComplete(): Unit = {
          if (!completed) {
            completed = true
            subscriber.onComplete()
            subscription = null
          }
        }
      })
    }
  }
  case class Cons[A](firstValue: A, underlying: Publisher[A]) extends Publisher[A] {
    override def subscribe(wrapped: Subscriber[_ >: A]): Unit = {
      underlying.subscribe(new Subscriber[A] {
        val pending = new AtomicBoolean(false)
        override def onSubscribe(wrappedSubscription: Subscription): Unit = {
          wrapped.onSubscribe(new Subscription {
            override def request(n: Long): Unit = {
              if (pending.compareAndSet(false, true)) {
                wrapped.onNext(firstValue)
                if (n > 1) {
                  if (n == Long.MaxValue) {
                    wrappedSubscription.request(n)
                  } else {
                    wrappedSubscription.request(n - 1)
                  }
                }
              } else {
                wrappedSubscription.request(n)
              }
            }
            override def cancel(): Unit = {
              wrappedSubscription.cancel()
            }
          })
        }
        override def onNext(t: A): Unit = {
          // this should never be the case, as request should always come before subscribe.
          // so ... if we're breaking the rules anyway, behaviour is 'undefined'.... so we'll
          // just push our first element and crack on
          if (pending.compareAndSet(false, true)) {
            wrapped.onNext(firstValue)
          }
          wrapped.onNext(t)
        }
        override def onError(t: Throwable): Unit = {
          wrapped.onError(t)
        }
        override def onComplete(): Unit = {
          wrapped.onComplete()
        }
      })
    }
  }

  case class Fixed[A](data: Iterable[A]) extends Publisher[A] {
    override def subscribe(s: Subscriber[_ >: A]): Unit = {
      s.onSubscribe(new Subscription() {
        val remaining = data.iterator
        var completed = false
        override def request(n: Long): Unit = {
          if (!completed) {
            doRequest(n)
          }
        }

        def doRequest(n: Long): Unit = {
          require(n > 0)
          var i = n
          while (i != 0 && remaining.hasNext) {
            val next = remaining.next()
            s.onNext(next)
            i = i - 1
          }
          if (!remaining.hasNext) {
            s.onComplete()
            completed = true
          }
        }
        override def cancel(): Unit = {}
      })
    }
  }
}
