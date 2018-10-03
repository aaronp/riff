package riff.reactive
import com.typesafe.scalalogging.StrictLogging
import org.reactivestreams.{Subscriber, Subscription}

case class BatchedSubscriber[A](
  wrapped: Subscriber[_ >: A],
  override val minimumRequestedThreshold: Int,
  override val subscriptionBatchSize: Int)
    extends SingleSubscriber[A] with StrictLogging { self =>
  override def onSubscribe(s: Subscription): Unit = {
    logger.info(s"onSubscribe($s)")
    super.onSubscribe(s)
    wrapped.onSubscribe(new Subscription {
      override def request(n: Long): Unit = {
        self.request(n)
      }
      override def cancel(): Unit = {
        self.cancel()
      }
    })
  }
  override protected def doOnNext(message: A): Unit = {
    logger.info(s"onNext($message)")
    wrapped.onNext(message)
  }
  override protected def doOnError(err: Throwable): Unit = {
    logger.info(s"onError($err)")
    wrapped.onError(err)
  }
  override protected def doOnComplete(): Unit = {
    logger.info(s"onComplete()")
    wrapped.onComplete()
  }
}
