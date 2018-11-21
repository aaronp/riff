package riff.reactive
import org.reactivestreams.Subscriber

/**
  * Tracks the state of the queued values to be pushed an the values requested
  *
  * @param s
  * @param totalRequested
  * @param valueQueue
  * @param cancelled
  * @tparam T
  */
private[reactive] case class AsyncSubscriptionState[T](
    s: Subscriber[_ >: T],
    totalRequested: Long,
    valueQueue: Vector[T],
    cancelled: Boolean,
    complete: Boolean,
    batchSize: Int = Int.MaxValue,
    completeSignaled: Boolean = false
) {
  require(totalRequested >= 0)

  private def pushNext(next: T) = {
    s.onNext(next)
  }

  override def toString = {
    val total = valueQueue.size
    val shortValues = if (total > 4) {
      valueQueue.take(4).mkString("[", ",", " ... ]")
    } else {
      valueQueue.mkString("[", ",", "]")
    }

    s"AsyncSubscriptionState(${total} values=$shortValues, totalRequested = ${totalRequested}, cancelled=$cancelled, complete=$complete, batchSize=$batchSize, completeSignaled=$completeSignaled)"

  }
  def continue = !done

  def done = {
    cancelled || (complete && completeSignaled && valueQueue.isEmpty)
  }

  import AsyncSubscriptionState._

  private def drainTheQueue(): AsyncSubscriptionState[T] = {
    val newState: AsyncSubscriptionState[T] = if (valueQueue.isEmpty) {
      this
    } else {
      //
      // again, max requested
      //
      valueQueue.foreach(pushNext)
      safeCopy(newRequested = Long.MaxValue, newQueue = Vector.empty[T])
    }

    newState.tryToComplete()
  }

  private[reactive] def update(next: Input[T]): AsyncSubscriptionState[T] = {
    next match {
      case Request(more) =>
        if (!cancelled) {
          onRequest(more)
        } else {
          this
        }
      case PushAll(values) => safeCopy(newQueue = valueQueue ++ values).onRequest(0)
      case Push(value) =>
        if (totalRequested > 0) {
          if (valueQueue.isEmpty) {
            // we're requesting but there are 0 values, so alert immediately and decrement the total count by 1
            pushNext(value)
            safeCopy(newRequested = totalRequested - 1)
          } else {
            // weird state - we're both requesting AND pushing more values -- ?
            // just update the valueQueue and poke the 'request' branch of logic to drive the subscription
            safeCopy(newQueue = valueQueue :+ value).onRequest(0)
          }
        } else {
          // nowt's requested - just valueQueue up
          safeCopy(newQueue = valueQueue :+ value)
        }
      case Cancel => safeCopy(newQueue = Vector.empty, isCancelled = true)
      case Error(exp) =>
        s.onError(exp)
        safeCopy(isCancelled = true)
      case Complete => safeCopy(isComplete = true).onRequest(0)
    }
  }

  /**
    * This is the primary branch for pushing values from the valueQueue. When new elements are added, or the valueQueue
    * is completed, then a call to '.onRequest(0)' is made to ensure this branch is executed, so any
    * draining, completing, whatever logic happens in this case
    *
    *
    * If there are MaxValues requested, then the subscription will be notified of ALL valueQueue values until the valueQueue
    * is empty.
    *
    */
  private def onRequest(more: Long): AsyncSubscriptionState[T] = {
    if (totalRequested == Long.MaxValue) {
      drainTheQueue()
    } else {
      val incrementedTotal: Long = totalRequested + more
      if (incrementedTotal < 0) {
        // we've rolled over Long.MaxValue -- so request Long.MaxValue and drain
        drainTheQueue()
      } else {

        // we an only split at an integer value from the vector
        val maxToTake                 = incrementedTotal.min(batchSize).toInt
        val (valuesToPush, remaining) = valueQueue.splitAt(maxToTake)
        val newTotal                  = incrementedTotal - valuesToPush.size

        valuesToPush.foreach(pushNext)

        val newState = safeCopy(newRequested = newTotal, newQueue = remaining)
        if (newTotal > 0 && remaining.nonEmpty) {
          //they've requested more than we can push from Int.MaxValue, so there's more to push
          newState.onRequest(0)
        } else {
          newState.tryToComplete()
        }
      }
    }
  }

  private def tryToComplete(): AsyncSubscriptionState[T] = {
    if (complete && !completeSignaled && valueQueue.isEmpty) {
      s.onComplete()
      safeCopy(newCompleteSignaled = true)
    } else {
      this
    }
  }
  private def safeCopy(newRequested: Long = totalRequested,
                       newQueue: Vector[T] = valueQueue,
                       isCancelled: Boolean = cancelled,
                       isComplete: Boolean = complete,
                       newCompleteSignaled: Boolean = completeSignaled) = {
    copy(totalRequested = newRequested, valueQueue = newQueue, cancelled = isCancelled, complete = isComplete, completeSignaled = newCompleteSignaled)
      .asInstanceOf[AsyncSubscriptionState[T]]
  }
}
private[reactive] object AsyncSubscriptionState {

  sealed trait Input[+T]
  case class Request(n: Long)                extends Input[Nothing]
  case class Push[T](value: T)               extends Input[T]
  case class PushAll[T](values: Iterable[T]) extends Input[T]
  case object Cancel                         extends Input[Nothing]
  case class Error(t: Throwable)             extends Input[Nothing]
  case object Complete                       extends Input[Nothing]

}
