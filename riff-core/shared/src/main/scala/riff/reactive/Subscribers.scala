package riff.reactive
import org.reactivestreams.{Subscriber, Subscription}

object Subscribers {

  case class NoOp[A]() extends Subscriber[A] {
    override def onSubscribe(s: Subscription): Unit = { s.cancel() }
    override def onNext(t: A): Unit = {}
    override def onError(t: Throwable): Unit = {}
    override def onComplete(): Unit = {}
  }

  def foreach[A](f: A => Unit): Subscriber[A] = new Subscriber[A] {
    private var errorHandler: Throwable => Unit = (_ : Throwable) => {}

    def withHandler(onErr : Throwable => Unit)= {
      errorHandler = onErr
    }
    override def onSubscribe(s: Subscription): Unit = {
      s.request(Long.MaxValue)
    }
    override def onNext(t: A): Unit = {
      f(t)
    }
    override def onError(t: Throwable): Unit = {
      errorHandler(t)
    }
    override def onComplete(): Unit = {}
  }
}
