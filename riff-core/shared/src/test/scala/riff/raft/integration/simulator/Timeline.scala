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

  /**
    * Directly insert an event at the given time. It will be inserted AFTER any events which already exist for the same
    * time.
    *
    * @param time the time at which to insert. If this is before the 'currentTime' and error is thrown
    * @param value the value to insert
    * @return the new timeline
    */
  def insertAtTime(time: Long, value: A): Timeline[A] = {
    require(time >= currentTime, s"Can't insert at $time as it is before $currentTime")
    val (before, after) = sortedEventsAscending.span(_._1 <= time)
    val entry           = (time, value)
    val newEvents       = before ::: entry :: after
    copy(sortedEventsAscending = newEvents)
  }

  /**
    * Inserts an event after a given delay (the delay being after the 'currentTime')
    *
    * The new event will be inserted after any events which occur at the same time.
    *
    * NOTE: If the event (the value) is a request, you would typically use 'pushAfter' to ensure
    * it comes after any already enqueued requests
    *
    * @param delay the delay at which to insert an event
    * @param value the event to insert
    * @return the new timeline coupled w/ the inserted event tuple (time and event)
    */
  def insertAfter(delay: FiniteDuration, value: A): (Timeline[A], (Long, A)) = {
    val time = currentTime + delay.toMillis
    insertAtTime(time, value) -> (time, value)
  }

  /**
    * When sending requests on a shared timeline, by default we want to maintain order. That is to say,
    * we don't want to by default insert a new request before another, already queued request was intended to be sent.
    *
    * Because: We're representing a timeline to represent a real scenario, in which case requests would be sent instead of
    * just put on a timeline. And so subsequently inserting a new request BEFORE an existing one gets popped off the timeline
    * is a misrepresentation of what would happen.
    *
    * @param value
    * @param predicate
    * @return
    */
  def pushAfter(delay: FiniteDuration, value: A)(predicate: PartialFunction[A, Boolean]): (Timeline[A], (Long, A)) = {
    val offset      = lastTimeMatching(predicate).getOrElse(currentTime)
    val time        = offset + delay.toMillis
    
    val newTimeline = insertAtTime(time, value)
    (newTimeline -> (time, value))
  }

  def pop(): Option[(Timeline[A], A)] = {
    sortedEventsAscending match {
      case (h @ (headTime, head: A)) :: tail =>
        Option(copy(currentTime = headTime, sortedEventsAscending = tail, historyDescending = h :: historyDescending) -> head)
      case Nil => None
    }
  }
  override def currentTimeline(): Timeline[A] = this
}

object Timeline {
  def apply[A](time: Long = 0) = new Timeline[A](time, time, Nil, Nil, Nil)
}
