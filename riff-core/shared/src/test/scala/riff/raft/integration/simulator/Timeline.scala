package riff.raft.integration.simulator

import scala.concurrent.duration.FiniteDuration

/**
  *  Used for testing as a means of walking through events (requests, responses, timeouts) in a controlled manner.
  *
  * @param initialTime the initial time when the timeline was started. An epoch, or typically just zero.
  * @param currentTime the current time in the timeline - this will be the time of the last popped event, and so initially is ste to initialTime
  * @param sortedEventsAscending the events in the timeline, coupled with the times when they take place.
  * @param history the past events in the timeline in reverse order
  * @tparam A
  */
case class Timeline[A] private (initialTime: Long,
                                currentTime: Long,
                                sortedEventsAscending: List[(Long, A)],
                                historyDescending: List[(Long, A)],
                                removed: List[(Long, A)])
    extends HasTimeline[A] {

  // just to help our sanity -- verify the events are always in increasing order
  sortedEventsAscending.sliding(2, 1).foreach {
    case List((time1, _), (time2, _)) => require(time1 <= time2, s"Invalid timeline $sortedEventsAscending")
    case _                            => require(sortedEventsAscending.size < 2)
  }

  def remove(entry: (Long, A)) = {
    if (sortedEventsAscending.contains(entry)) {
      copy(sortedEventsAscending = sortedEventsAscending diff List(entry), removed = entry :: removed)
    } else {
      this
    }
  }

  def wasRemoved(predicate : PartialFunction[A, Boolean]) = removed.map(_._2).exists { a =>
    predicate.isDefinedAt(a) && predicate(a)
  }

  def insertAtTime(time: Long, value: A): Timeline[A] = {
    require(time >= currentTime, s"Can't insert at $time as it is before $currentTime")
    val (before, after) = sortedEventsAscending.span(_._1 <= time)
    val entry           = (time, value)
    val newEvents       = before ::: entry :: after
    copy(sortedEventsAscending = newEvents)
  }

  def insertAfter(delay: FiniteDuration, value: A): (Timeline[A], (Long, A)) = {
    val time = currentTime + delay.toMillis
    insertAtTime(time, value) -> (time, value)
  }

  def pop(): Option[(Timeline[A], A)] = {
    sortedEventsAscending match {
      case (h @ (headTime, head : A)) :: tail =>
        Option(copy(currentTime = headTime, sortedEventsAscending = tail, historyDescending = h :: historyDescending) -> head)
      case Nil => None
    }
  }
  override def currentTimeline(): Timeline[A] = this
}

object Timeline {
  def apply[A](time: Long = 0) = new Timeline[A](time, time, Nil, Nil, Nil)
}
