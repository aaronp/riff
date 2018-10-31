package riff.reactive

import com.typesafe.scalalogging.StrictLogging
import org.reactivestreams.{Publisher, Subscriber}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext

/**
  * A hot publisher to multiple subscribers, where new subscriptions simply begin observing the messages as the point of
  * subscription. Each subscription maintains its own queue, and so slow consumers can end up eating up memory.
  *
  * This is quite a basic implementation of a reactive-stream, without real attention to efficiency or performance.
  *
  * Consider using the raft-monix, raft-fs2 or raft-akka libraries for a more robust approach.
  *
  * @param underlying the handler which will handle the messages
  * @param ctxt the execution context used for each subscriber to consumer its messages
  * @tparam A the type of data which is appended to the log (could just be a byte array, some union type, etc)
  */
trait AsyncPublisher[T] extends Publisher[T] with StrictLogging with AutoCloseable {
  implicit def ctxt: ExecutionContext

  private val subscriptions = ListBuffer[AsyncSubscription[T]]()
  private var errorOpt: Option[Throwable] = None

  /** We enqueue request(n), push, cancel and completed requests as instructions to the subscribers.
    * These instructions are kept in a queue with this as its max size
    * @return the queue size for the subscriber messages (push, request, cancel, etc)
    */
  protected def maxQueueSize: Int

  protected def add(sub: AsyncSubscription[T]): AsyncSubscription[T] = {
    subscriptions.synchronized {
      subscriptions += sub
    }
    sub
  }
  protected def remove(sub: AsyncSubscription[T]): AsyncSubscription[T] = {
    subscriptions.synchronized {
      subscriptions -= sub
    }
    sub
  }

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
  final def subscribeWith[S <: Subscriber[_ >: T]](s: S): S = {
    subscribe(s)
    s
  }

  override def subscribe(s: Subscriber[_ >: T]): Unit = {
    logger.debug(s"subscribe($s)")
    errorOpt match {
      case Some(err) =>
        s.onSubscribe(Publishers.NoOpSubscription)
        s.onError(err)
      case None =>
        val asyncSub = newAsyncSubscription(s, remove _)
        add(asyncSub)
        s.onSubscribe(asyncSub)
    }
  }

  // protected scope added for test, so we can drive the subscription via the test
  protected def newAsyncSubscription(s: Subscriber[_ >: T], onCancel: AsyncSubscription[T] => Unit): AsyncSubscription[T] = {
    AsyncSubscription[T](s, maxQueueSize, onCancel)
  }

  protected def enqueueError(result: Throwable): Unit = {
    errorOpt = errorOpt.orElse(Option(result))
    subscriptions.foreach(_.cancel())
  }
  protected def enqueueMessages(messages: Iterable[T]): Unit = {
    subscriptions.foreach(_.enqueueAll(messages))
  }
  protected def enqueueMessage(message: T): Unit = {
    subscriptions.foreach(_.enqueue(message))
  }
  protected def enqueueComplete(): Unit = {
    subscriptions.foreach(_.complete())
  }
  protected def enqueueCancel(): Unit = {
    subscriptions.foreach(_.cancel())
  }

  override def close() = {
    enqueueComplete()
  }
}
