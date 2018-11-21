package riff.reactive
import org.reactivestreams.{Subscriber, Subscription}

object Subscribers {

  case class NoOp[A]() extends Subscriber[A] {
    override def onSubscribe(s: Subscription): Unit = { s.cancel() }
    override def onNext(t: A): Unit = {}
    override def onError(t: Throwable): Unit = {}
    override def onComplete(): Unit = {}
  }

}
