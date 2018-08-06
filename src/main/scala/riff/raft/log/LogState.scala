package riff.raft.log

final case class LogState(commitIndex: Int, latestTerm: Int, latestIndex: Int)

object LogState {

  val Empty = LogState(0, 0, 0)
}
