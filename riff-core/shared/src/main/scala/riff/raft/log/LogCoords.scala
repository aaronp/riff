package riff.raft.log
import riff.raft.{LogIndex, Term}

/**
  * Represents the coords of a log entry
  *
  * @param term
  * @param index
  */
final case class LogCoords(term: Term, index: LogIndex) {
  require(term >= 0)
  require(index >= 0)
  override def toString = s"{term=$term, index=$index}"
  def asKey             = s"${term}:${index}"
}

object LogCoords {
  val Empty = LogCoords(0, 0)

  private val LatestAppended = """([0-9]+):([0-9]+)""".r

  object FromKey {
    def unapply(key: String) = {
      key match {
        case LatestAppended(t, i) => Some(LogCoords(t.toInt, i.toInt))
        case _                    => None
      }
    }
  }
}
