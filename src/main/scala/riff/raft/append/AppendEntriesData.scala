package riff.raft.append
import riff.raft.log.LogCoords

/**
  *
  * @param previous the previous log entry preceeding the entries
  * @param term the leader's term
  * @param commitIndex the latest commit index of the leader
  * @param entries the entries to append
  * @tparam T
  */
final case class AppendEntriesData[T](previous: LogCoords, term: Int, commitIndex: Int, entries: Array[Entry[T]]) {
  def isHeartbeat: Boolean = entries.isEmpty
}
