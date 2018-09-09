package riff.raft.integration

import scala.concurrent.duration.FiniteDuration

/**
  * Used for testing as a means of walking through events (requests, responses, timeouts) in a controlled manner.
  *
  */
case class Timeline private (currentTime: Long, sortedEventsAscending: List[(Long, Any)]) {

  // just to help our sanity -- verify the events are always in increasing order
  sortedEventsAscending.sliding(2, 1).foreach {
    case List((time1, _), (time2, _)) => require(time1 <= time2, s"Invalid timeline $sortedEventsAscending")
    case _                            => require(sortedEventsAscending.size < 2)
  }

  def diff(other: Timeline): List[(Long, Any)] = sortedEventsAscending.diff(other.sortedEventsAscending)

  def remove(entry: (Long, Any)) = {
    copy(sortedEventsAscending = sortedEventsAscending diff List(entry))
  }

  def events = sortedEventsAscending

  def insertAtTime(time: Long, value: Any): Timeline = {
    require(time > currentTime, s"Can't insert at $time as it is before $currentTime")
    val (before, after) = sortedEventsAscending.span(_._1 <= time)
    val entry           = (time, value)
    val newEvents       = before ::: entry :: after
    copy(sortedEventsAscending = newEvents)
  }

  def insertAfter(delay: FiniteDuration, value: Any): (Timeline, (Long, Any)) = {
    val time = currentTime + delay.toMillis
    insertAtTime(time, value) -> (time, value)
  }

  def pop(): Option[(Timeline, Any)] = {
    sortedEventsAscending match {
      case (headTime, head) :: tail =>
        Option(copy(currentTime = headTime, sortedEventsAscending = tail) -> head)
      case Nil => None
    }
  }
}

object Timeline {
  def apply(time: Long = 0) = new Timeline(time, Nil)

  class Shared(initial: Timeline) {
    var current = Timeline
  }

}
