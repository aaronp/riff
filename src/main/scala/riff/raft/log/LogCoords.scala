package riff.raft.log

/**
  * Represents the coords of a log entry
  * @param term
  * @param index
  */
final case class LogCoords(term: Int, index: Int) extends Ordered[LogCoords] {
  require(term >= 0)
  require(index >= 0)
  def inc: LogCoords = copy(index + 1)
  override def compare(that: LogCoords): Int = {
    term.compareTo(that.term) match {
      case 0 => index.compareTo(that.index)
      case n => n
    }
  }

}

object LogCoords {
  val Empty = LogCoords(0, 0)
}
