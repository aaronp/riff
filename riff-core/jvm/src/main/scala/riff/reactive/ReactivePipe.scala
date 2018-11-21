package riff.reactive
import org.reactivestreams.{Publisher, Subscriber}

import scala.concurrent.ExecutionContext

/**
  * Represents an input/output pipe
  *
  * @param input
  * @param output
  * @param ev$1
  * @param ev$2
  * @tparam Sub
  * @tparam Pub
  */
case class ReactivePipe[In, Out, Sub[_]: AsSubscriber, Pub[_]: AsPublisher](input: Sub[In], output: Pub[Out]) extends AutoCloseable {

  /**
    * Convenience method for 'subscribe', but which returns the subscriber instance:
    * {{{
    *   val mySub = publisher.subscribeWith(new FooSubscriber) // mySub will be a FooSubscriber
    * }}}
    *
    * @param s the subscriber to return
    * @tparam S the subscriber type
    * @return the same subscriber
    */
  final def subscribeWith[S <: Subscriber[_ >: Out]](s: S): S = {
    implicitly[AsPublisher[Pub]].subscribeWith(output, s)
  }

  def outputPublisher: Publisher[Out] = AsPublisher[Pub].asPublisher(output)
  def inputSubscriber: Subscriber[In] = AsSubscriber[Sub].asSubscriber(input)
  override def close(): Unit = {
    input match {
      case closable: AutoCloseable => closable.close()
      case _                       =>
    }
    output match {
      case closable: AutoCloseable => closable.close()
      case _                       =>
    }
  }
}

object ReactivePipe {

  def multi[A](queueSize: Int, delayErrors: Boolean, minimumRequestedThreshold: Int = 10, subscriptionBatchSize: Int = 100)(
      implicit executionContext: ExecutionContext): ReactivePipe[A, A, Subscriber, Publisher] = {
    val feedAndSink: ReactivePipe[A, A, Subscriber, Publisher] =
      single(queueSize, minimumRequestedThreshold, subscriptionBatchSize)
    val multiSubscriber = MultiSubscriberProcessor[A](queueSize, delayErrors)

    // here the feedAndSink.input will be fed from multiple feeds
    multiSubscriber.subscribe(feedAndSink.input)
    new ReactivePipe[A, A, Subscriber, Publisher](multiSubscriber, feedAndSink.output)
  }

  def single[A](maxQueueSize: Int, minimumRequestedThreshold: Int, subscriptionBatchSize: Int)(
      implicit executionContext: ExecutionContext): ReactivePipe[A, A, Subscriber, Publisher] = {
    val feedAndSink = new Instance[A](maxQueueSize, minimumRequestedThreshold, subscriptionBatchSize)
    new ReactivePipe[A, A, Subscriber, Publisher](feedAndSink, feedAndSink)
  }

  class Instance[A](override val maxQueueSize: Int, override protected val minimumRequestedThreshold: Int, override protected val subscriptionBatchSize: Int)(
      override implicit val ctxt: ExecutionContext)
      extends AsyncPublisher[A] with SingleSubscriber[A] with AutoCloseable {
    require(minimumRequestedThreshold <= subscriptionBatchSize)
    require(minimumRequestedThreshold >= 0)
    override protected def doOnNext(message: A): Unit      = enqueueMessage(message)
    override protected def doOnError(err: Throwable): Unit = enqueueError(err)
    override def doOnComplete(): Unit                      = enqueueComplete()
    override def close()                                   = onComplete()
  }
}
