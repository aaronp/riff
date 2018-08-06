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
}

object LogCoords {
  val Empty = LogCoords(0, 0)
}
