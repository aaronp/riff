package riff.reactive

import org.reactivestreams.{Subscriber, Subscription}

/**
  * Adds a means to accessing the Subscription when subscribed.
  *
  * The 'onError' will remove the subscription, so overriding classes should implement 'doOnError'
  * for error-handling behvior.
  *
  *
  */
trait SingleSubscriber[A] extends Subscriber[A] {

  /** @return the number of elements requested from its subscription
    */
  protected def subscriptionBatchSize: Int

  /** @return the number of elements received of the 'subscriptionBatchSize', below which another 'request(<subscriptionBatchSize>)' request is made
    */
  protected def minimumRequestedThreshold: Int

  private var receivedCounter = subscriptionBatchSize - minimumRequestedThreshold

  protected def doOnNext(message: A): Unit

  protected def doOnError(err: Throwable): Unit // = {}

  protected def doOnComplete(): Unit

  private var subscriptionOpt: Option[Subscription] = None
  override def onSubscribe(s: Subscription): Unit = {
    subscriptionOpt match {
      case Some(_) => s.cancel()
      case None =>
        if (subscriptionBatchSize > 0) {
          s.request(subscriptionBatchSize)
        }
        subscriptionOpt = Option(s)
    }
  }

  /** @param n the number of elements to request
    * @return true if there was a subscription from which to request, false otherwise
    */
  def request(n: Long) = {
    subscriptionOpt.fold(false) { s =>
      s.request(n)
      true
    }
  }

  def cancel() = subscriptionOpt.fold(false) { s =>
    s.cancel()
    subscriptionOpt = None
    true
  }
  override final def onNext(nextMsg: A): Unit = {
    if (nextMsg == null) {
      throw new NullPointerException
    }

    subscriptionOpt.foreach { s =>
      receivedCounter = receivedCounter - 1
      if (receivedCounter == 0) {
        receivedCounter = subscriptionBatchSize - minimumRequestedThreshold
        s.request(subscriptionBatchSize)
      }
    }
    doOnNext(nextMsg)
  }
  final override def onError(t: Throwable): Unit = {
    if (t == null) {
      throw new NullPointerException
    }
    doOnError(t)
    subscriptionOpt = None
  }
  override final def onComplete(): Unit = {
    subscriptionOpt = None
    doOnComplete()
  }
}
