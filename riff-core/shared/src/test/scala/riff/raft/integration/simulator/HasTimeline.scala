package riff.raft.integration.simulator
import scala.reflect.ClassTag

/**
  * Adds some functionality for anything which has a 'currentTimeline'
  *
  * @tparam A
  */
trait HasTimeline[A] {

  def currentTimeline(): Timeline[A]

  def timeline(): List[(Long, A)] = currentTimeline.events

  def timelineValues(): List[A] = timeline().map(_._2)

  def findOnly[T <: A: ClassTag]: (Long, T) = {
    val List(only) = findAll[T]
    only
  }

  def findAll[T <: A: ClassTag]: List[(Long, T)] = timeline().collect {
    case (time, tea: T) => (time, tea)
  }

  def pretty(indent: String = ""): String = {
    val sortedEventsAscending = currentTimeline.sortedEventsAscending
    val currentTime           = currentTimeline.currentTime
    sortedEventsAscending.headOption.fold(s"empty timeline @ $currentTime") {
      case (firstTime, firstEvent) =>
        val padded = {

          val strings = (s"$indent$currentTime", firstEvent.toString) +: sortedEventsAscending.tail.map {
            case (time, event) =>
              s"$indent+${time - firstTime}ms" -> event.toString
          }
          val padSize = strings.map(_._1.length).max
          strings.map {
            case (time, event) => s"${time.padTo(padSize, ' ')} : $event"
          }
        }

        padded.mkString(indent, s"\n$indent", "\n")
    }
  }
}
