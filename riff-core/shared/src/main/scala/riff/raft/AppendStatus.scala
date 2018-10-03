package riff.raft
import riff.raft.log.LogAppendSuccess

/**
  * Represents the current state of the cluster following an append
  *
  * @param logCoords the coords of the entry appended
  * @param appended a map of the cluster ids to a flag indicating whether or not the node has appended the entry
  * @param committed a flag to indicate whether the entry has been committed on the leader
  * @param clusterSize the size of the cluster
  */
final case class AppendStatus(
  leaderAppendResult: LogAppendSuccess,
  appended: Map[String, Boolean],
  committed: Boolean,
  clusterSize: Int) {

  def withResult(from: NodeId, ok: Boolean) = {
    require(!appended.contains(from), s"Already got a response from $from")
    copy(appended = appended.updated(from, ok))
  }
}