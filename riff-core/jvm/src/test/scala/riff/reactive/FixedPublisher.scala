package riff.reactive
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import org.reactivestreams.{Publisher, Subscriber, Subscription}

object FixedPublisher {

  def apply[T](first: T, theRest: T*): FixedPublisher[T] = {
    new FixedPublisher(first +: theRest.toList, true)
  }
}
case class FixedPublisher[T](values: Iterable[T], allowOnComplete: Boolean) extends Publisher[T] {
  override def subscribe(s: Subscriber[_ >: T]): Unit = {
    s.onSubscribe(new Subscription {
      var remaining = values.toList
      var pending   = new AtomicLong(0)
      var inProcess = new AtomicBoolean(false)

      // if we're not careful we can form an infinite loop here if the subscriber calls 'request' from its onNext
      override def request(n: Long): Unit = {
        if (inProcess.compareAndSet(false, true)) {
          feed(n)
          inProcess.compareAndSet(true, false)
          if (pending.get > 0 && remaining.nonEmpty) {
            request(pending.get)
          }
        } else {
          pending.addAndGet(n)
        }
      }

      def feed(n: Long): Unit = {
        val (taken, left) = if (n >= Int.MaxValue) {
          (remaining, Nil)
        } else {
          remaining.splitAt(n.toInt)
        }
        remaining = left
        taken.foreach(s.onNext)
        if (allowOnComplete && left.isEmpty) {
          s.onComplete()
        }
      }
      override def cancel(): Unit = {
        remaining = Nil
      }
    })
  }
}
