package riff.raft.integration.simulator

import scala.concurrent.duration.FiniteDuration

/**
  * Used for testing as a means of walking through events (requests, responses, timeouts) in a controlled manner.
  *
  */
case class Timeline[A] private (initialTime: Long, currentTime: Long, sortedEventsAscending: List[(Long, A)]) extends HasTimeline[A] {

  // just to help our sanity -- verify the events are always in increasing order
  sortedEventsAscending.sliding(2, 1).foreach {
    case List((time1, _), (time2, _)) => require(time1 <= time2, s"Invalid timeline $sortedEventsAscending")
    case _                            => require(sortedEventsAscending.size < 2)
  }

  def diff(other: Timeline[A]): List[(Long, A)] = sortedEventsAscending.diff(other.sortedEventsAscending)

  def remove(entry: (Long, A)) = {
    copy(sortedEventsAscending = sortedEventsAscending diff List(entry))
  }

  def size() = sortedEventsAscending.size

  def events: List[(Long, A)] = sortedEventsAscending

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
      case (headTime, head) :: tail =>
        Option(copy(currentTime = headTime, sortedEventsAscending = tail) -> head)
      case Nil => None
    }
  }
  override def currentTimeline(): Timeline[A] = this
}

object Timeline {
  def apply[A](time: Long = 0) = new Timeline[A](time, time, Nil)
}
