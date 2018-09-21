package riff.raft.integration.simulator
import scala.reflect.ClassTag

/**
  * Adds some functionality for anything which has a 'currentTimeline'
  *
  * @tparam A
  */
trait HasTimeline[A] {

  def currentTimeline(): Timeline[A]

  def diff(other: Timeline[A]): List[(Long, A)] = currentTimeline.sortedEventsAscending.diff(other.sortedEventsAscending)

  def size() = events.size

  def events: List[(Long, A)] = currentTimeline.sortedEventsAscending

  def timelineValues(): List[A] = events.map(_._2)

  def lastTimeMatching(predicate: PartialFunction[A, Boolean]): Option[Long] = {
    events.reverse.collectFirst {
      case (time, a) if predicate.isDefinedAt(a) && predicate(a) => time
    }
  }

  /** A convenience method for dumping the limeline as an expectation.
    * Useful for stepping through test scenarios and adding the results as an assertion
    *
    * @param ev
    * @tparam T
    * @return the current timeline as a String which can be pasted
    */
  def timelineAsExpectation[T](prefix : String = "simulator.timelineAssertions shouldBe ")(implicit ev: A =:= TimelineType) = {
    val quote = "\""
    val code = timelineAssertions(ev).mkString(s"$prefix List(\n    $quote", "\",\n    \"", "\"\n)")

    s"""
      |withClue(simulator.timelineAsExpectation()) {
      |  $code
      |}
    """.stripMargin
  }

  def timelineAssertions[T](implicit ev: A =:= TimelineType) = {
    timelineValues().map { x =>
      ev(x).asAssertion()
    }
  }

  def findOnly[T <: A: ClassTag]: (Long, T) = {
    val List(only) = findAll[T]
    only
  }

  def findAll[T <: A: ClassTag]: List[(Long, T)] = events.collect {
    case (time, tea: T) => (time, tea)
  }

  def pretty(indent: String = ""): String = {
    val timeline = currentTimeline

    val currentTime = timeline.currentTime

    val all = {

      val (pastDeleted, futureDeleted) = {
        val deleted = timeline.removed.sortBy(_._1).map {
          case (time, event) => (time, s"(removed) $event")
        }
        deleted.partition(_._1 < currentTime)
      }

      implicit val ord: Ordering[(Long, Any)] = Ordering.by[(Long, Any), Long](_._1)
      val hist                                = MergeSorted(currentTimeline.historyDescending.reverse, pastDeleted)
      val future                              = MergeSorted(timeline.sortedEventsAscending, futureDeleted)
      (hist ::: future).map {
        case (time, event) =>
          val sign = if (time < currentTime) "-" else if (time > currentTime) "+" else "@"
          val diff = (currentTime - time).abs
          s"$sign${diff}ms" -> event
      }
    }

    if (all.isEmpty) {
      s"${indent}empty timeline @ $currentTime"
    } else {
      val padded = {
        val padSize = all.map(_._1.length).max
        all.map {
          case (time, event) => s"${time.padTo(padSize, ' ')} : $event"
        }
      }
      padded.mkString(indent, s"\n$indent", "\n")
    }
  }


  def wasRemoved(predicate: PartialFunction[A, Boolean]): Boolean = {
    // format: off
    currentTimeline().removed.exists {
      case (_, a) => predicate.isDefinedAt(a) && predicate(a)
    }
    // format: on
  }
}
