package riff.raft
import riff.raft.log.{LogAppendSuccess, LogCoords}
import riff.raft.messages.AppendEntriesResponse

/**
  * Represents the current state of the cluster following an append
  *
  * @param logCoords the coords of the entry appended
  * @param appended a map of the cluster ids to a flag indicating whether or not the node has appended the entry
  * @param appendedCoords the log coords which have been appended on the leader
  * @param clusterSize the size of the cluster
  * @param committed the coordinates of committed entries on the leader after quarum has ack'd
  * @param errorAfterAppend A means of letting observers know that a log append error has occurred, perhaps due to a leader change
  */
final case class AppendStatus(leaderAppendResult: LogAppendSuccess,
                              appended: Map[NodeId, AppendEntriesResponse],
                              appendedCoords: Set[LogCoords],
                              clusterSize: Int,
                              committed: Set[LogCoords],
                              errorAfterAppend: Option[Exception] = None) {

  def allCommitted: Boolean = committed == appendedCoords

  override def toString: String = {

    val responses = appended.map {
      case (name, resp) => s"\t$name : $resp"
    }

    s"""AppendStatus(
       |  leaderAppendResult = $leaderAppendResult,
       |  clusterSize = $clusterSize,
       |  ${appendedCoords.size} appendedCoords = $appendedCoords,
       |  ${appended.size} appended: ${responses.mkString("(\n", ",\n", "),")}
       |  ${committed.size} committed = ${committed.mkString("[", ",", "]")})
       |  errorAfterAppend = $errorAfterAppend""".stripMargin
  }

  /** Meant to signal that we have all that we expect to -- acks from EVERY node in the cluster and the entries are committed.
    *
    * If a node is down, OR A LEADER CHANGE HAPPENS, CAUSING NOT ALL NODES TO ACK AN APPEND, then this may never be true.
    *
    * In the case, however, that not all nodes respond, we will notice the 'errorAfterAppend' field become set
    *
    * @return true if we've received all the messages expected
    */
  def isComplete: Boolean = {
    numberOfPeersContainingCommittedIndex == clusterSize && allCommitted
  }

  def numberOfPeersContainingCommittedIndex: Int = appended.values.count { resp => //
    resp.success && appendedCoords.contains(resp.coords)
  }

  def withResult(from: NodeId, response: AppendEntriesResponse): AppendStatus = {
    copy(appended = appended.updated(from, response))
  }

  def withCommit(coords: LogCoords): AppendStatus = {
    require(appendedCoords.contains(coords), "Attempt to add committed coors for entries we've not appended")
    copy(committed = committed + coords)
  }
}
