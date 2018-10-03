package riff.reactive
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
