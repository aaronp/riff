package riff.raft.log
import riff.raft.{LogIndex, Term}

final case class LogState(commitIndex: LogIndex, latestTerm: Term, latestIndex: LogIndex)

object LogState {

  val Empty = LogState(0, 0, 0)
}
