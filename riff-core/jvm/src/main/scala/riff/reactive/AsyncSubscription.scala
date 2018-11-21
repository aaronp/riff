package riff.reactive
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.StrictLogging
import org.reactivestreams.{Subscriber, Subscription}

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

/**
  * Represents a subscription whose actions are handled via a Runnable pulling values from a blocking internalStateInputQueue submitted on
  * a provided execution context
  *
  * @param s the subscriber callback
  * @param onCancel some function to invoke when the subscription is cancelled
  * @tparam T
  */
final class AsyncSubscription[T] private[reactive] (private[reactive] val s: Subscriber[_ >: T], maxQueueSize: Int, onCancel: AsyncSubscription[T] => Unit)
    extends Subscription with StrictLogging {

  private val createdFrom = Thread.currentThread().getStackTrace.mkString("\n")

  type Input = AsyncSubscriptionState.Input[T]
  private val internalStateInputQueue: BlockingQueue[Input] = new ArrayBlockingQueue[Input](maxQueueSize)
  private var state                                         = AsyncSubscriptionState[T](s, 0L, Vector.empty[T], false, false)

  override def toString = {
    import scala.collection.JavaConverters._
    internalStateInputQueue.asScala.mkString("AsyncSubscription [", ", ", "]")
  }

  def enqueue(msg: T) = {
    doEnqueue(AsyncSubscriptionState.Push(msg))
  }

  def enqueueErr(err: Throwable) = {
    doEnqueue(AsyncSubscriptionState.Error(err))
  }

  def enqueueAll(msg: Iterable[T]) = {
    val input: Input = AsyncSubscriptionState.PushAll[T](msg)
    doEnqueue(input)
  }

  def complete(): Unit = {
    doEnqueue(AsyncSubscriptionState.Complete)
  }

  override def request(n: Long): Unit = {
    if (n <= 0) {
      val err = new IllegalArgumentException(s"request must request positive values, not $n")
      cancel()
      s.onError(err)
    } else {
      doEnqueue(AsyncSubscriptionState.Request(n))
    }
  }

  override def cancel(): Unit = {
    internalStateInputQueue.clear()
    doEnqueue(AsyncSubscriptionState.Cancel)
  }

  private def remove() = {
    onCancel(this)
  }

  private[reactive] def updateState(next: Input) = {
    val before = state
    state = state.update(next)
    logger.debug(s"Updated \n$before to \n$state\n w/ $next")
    state
  }

  private[reactive] def currentState(): AsyncSubscriptionState[T] = state

  private[reactive] def inputQueueSize() = internalStateInputQueue.size

  private[reactive] def popNextInput(): Input = {
    logger.debug(s"Taking next from internalStateInputQueue w/ ${internalStateInputQueue.size} elements")
//    val next = internalStateInputQueue.take()
    var next = internalStateInputQueue.poll(1, TimeUnit.SECONDS)
    while (next == null && !state.done) {
      logger.debug(s"Waiting for value w/ $state")
      next = internalStateInputQueue.poll(1, TimeUnit.SECONDS)
    }
    logger.trace(s"took $next, internalStateInputQueue is now ${internalStateInputQueue.size}")
    next
  }

  private[reactive] def canContinue() = state.continue

  private def doEnqueue(input: Input) = {

    logger.debug(s"enqueueing '$input' to queue ${internalStateInputQueue.size()}")
    try {
      internalStateInputQueue.add(input)
    } catch {
      case NonFatal(err) =>
        logger.warn(s"error enqueueing '$input' w/ maxQueueSize=$maxQueueSize: $err\nWHICH WAS CREATED FROM:\n$createdFrom\n\n\n\n")
        cancel()
        s.onError(err)
    }
  }

  /** This is a blocking call, and it intended to be driven by the 'Drain' task executed in the companion object.
    *
    * The next input is (blocking) taken from the internalStateInputQueue and then applied to the current state.
    *
    * @return true if this subscription should continue to pull elements from the internalStateInputQueue, false otherwise
    */
  private[reactive] def processNext(): Boolean = {
    val next = popNextInput()
    if (next != null) {
      updateState(next)
      canContinue()
    } else {
      false
    }
  }

}

object AsyncSubscription {

  private val subIdCounter = new AtomicInteger(0)
  private def newAsyncExecContext(name: String = s"AsyncSubscription-${subIdCounter.incrementAndGet()}") = {
    val service = Executors.newSingleThreadExecutor(new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val t = new Thread(r)
        t.setName(name)
        t.setDaemon(true)
        t
      }
    })
    ExecutionContext.fromExecutor(service)
  }

  def apply[T](s: Subscriber[_ >: T], maxQueueSize: Int, onCancel: AsyncSubscription[T] => Unit): AsyncSubscription[T] = {
    val sub = new AsyncSubscription[T](s, maxQueueSize, onCancel)

    /**
      * This Runnable simply drives our new AsyncSubscription's 'processNext' blocking queue.
      */
    object Drain extends Runnable with StrictLogging {
      override def run(): Unit = {
        try {
          while (sub.processNext()) {
            // processNext has already done the work, just loop and pull the next input
          }
        } catch {
          case NonFatal(e) =>
            logger.error(s"on error: $e")
            sub.cancel()
            // undefined - the subscription shouldn't throw
            s.onError(e)
        } finally {
          sub.remove()
        }
      }
    }

    implicit val ctxt: ExecutionContext = newAsyncExecContext()
    ctxt.execute(Drain)

    sub

  }
}
