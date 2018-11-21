package riff.reactive
import com.typesafe.scalalogging.StrictLogging
import org.reactivestreams.{Subscriber, Subscription}

import scala.concurrent.ExecutionContext

object MultiSubscriberProcessor {

  def apply[A](queueSize: Int, delayErrorsFromSubscribers: Boolean)(implicit execContext: ExecutionContext): MultiSubscriberProcessor[A] = {
    new MultiSubscriberProcessor[A] {
      override def maxQueueSize: Int               = queueSize
      override def delayErrors: Boolean            = delayErrorsFromSubscribers
      override implicit def ctxt: ExecutionContext = execContext
    }
  }
}

/**
  * A 'Processor' (both a subscriber and publisher) which can be subscribed to multiple publishers in order to collate and publish their values.
  *
  * {{{
  *   val sub : MultiSubscriberProcessor[A] = ...
  *   pub1.subscribe(sub)
  *   pub2.subscribe(sub)
  *   pub3.subscribe(sub)
  *
  *   // the subscribers to 'sub' will receive the values published from pub1, pub2 and pub3
  *   sub.subscribe(...)
  * }}}
  *
  * @param delayErrors if false, errors from any publishers will be propagated to subscribers to this MultiSubscriberProcessor
  */
trait MultiSubscriberProcessor[A] extends AsyncPublisher[A] with Subscriber[A] with StrictLogging with AutoCloseable {

  /** @return true if we should suppress errors from any one subscription
    */
  def delayErrors: Boolean

  private var errorOpt: Option[Throwable] = None

  override def onSubscribe(s: Subscription): Unit = {
    logger.debug(s"onSubscribe($s)")
    s.request(Long.MaxValue)
  }
  override final def onNext(t: A): Unit = {
    logger.debug(s"onNext($t)")
    enqueueMessage(t)
  }
  override def onError(t: Throwable): Unit = {
    logger.debug(s"onError($t)")
    if (!delayErrors) {
      enqueueError(t)
    }
    errorOpt = errorOpt.orElse(Option(t))
  }
  override def onComplete(): Unit = {
    logger.debug(s"onComplete() - ignored")
    // ignore, as we may subscribe to something else
  }

  /**
    * Exposes a means to force complete immediately this subscriber.
    *
    * This is necessary as a multi-subscriber, by definition, can be reused and
    * handed to a publisher at any time, so this will explicitly complete and downstream
    * subscribers and thus release any resources used.
    */
  def complete(propagateErrors: Boolean = true) = {
    errorOpt match {
      case Some(err) if propagateErrors =>
        enqueueError(err)
      case _ =>
        enqueueComplete()
    }
  }

  /**
    * Marks this subscription in error. Similar to 'complete', this is required as this subscription
    * will be subscribed to ma
    *
    * @param exp
    */
  def inError(exp: Throwable) = {
    enqueueError(exp)
  }

  override def close() = {
    enqueueComplete()
  }

}
