package riff.raft
import riff.raft.log.{LogAppendSuccess, LogCoords}
import riff.raft.messages.{AddressedMessage, AppendEntriesResponse, RaftMessage}
import riff.raft.node.{LeaderCommittedResult, RaftNodeResult}
import riff.reactive.AsPublisher

/**
  * Represents the current state of the cluster following an append
  *
  * @param logCoords the coords of the entry appended
  * @param appended a map of the cluster ids to a flag indicating whether or not the node has appended the entry
  * @param appendedCoords the log coords which have been appended on the leader
  * @param clusterSize the size of the cluster
  */
final case class AppendStatus(leaderAppendResult: LogAppendSuccess, appended: Map[NodeId, AppendEntriesResponse], appendedCoords: Set[LogCoords], clusterSize: Int) {

  /** @return true if the leader has committed the latest (highest) coordinate of the appended log entries
    */
  def committed: Boolean = appendedCoords.contains(leaderAppendResult.lastIndex)

  /** @return true if we've received all the messages expected
    */
  def isComplete: Boolean = {
    numberOfPeersContainingCommittedIndex == clusterSize
  }

  def numberOfPeersContainingCommittedIndex: Int = appended.values.count { resp => //
    resp.success && appendedCoords.contains(resp.coords)
  }

  def withResult(from: NodeId, response: AppendEntriesResponse, newlyAppended: Seq[LogCoords]): AppendStatus = {
    copy(appended = appended.updated(from, response), appendedCoords = appendedCoords ++ newlyAppended)
  }
}

object AppendStatus {

  import AsPublisher.syntax._

  /**
    * Create a publisher of [[AppendStatus]] which a client may observe.
    *
    * This forms arguably the essential, primary use-case in using riff, as client code can then take the decisions based
    * on the provided feed to wait for quorum, full replication, or even just this node's initial AppendStatus (e.g. having data only on this leader node),
    * etc.
    *
    *
    * Essentially the publisher will be a filtered stream of the node's input which collects [[AppendEntriesResponse]]s
    * relevant to the [[riff.raft.messages.AppendData]] result which returned the given 'logAppendSuccess'.
    *
    * Given that [[riff.raft.messages.AppendEntries]] requests can send multiple entries in a configurable batch, this
    * function should be able to cope with a 'zero to many' cardinality of [[AppendEntriesResponse]]s
    *
    * @param nodeId the nodeId of the leader node whose logAppendSuccess and nodeInput are given, used to populate the AppendStatus
    * @param clusterSize the cluster size, used to populate the AppendStatus entries
    * @param logAppendSuccess the result from this leader's log append (as a result of having processed an [[riff.raft.messages.AppendData]] request
    * @param nodeInput the zipped input of request/responses for a target node (which ultimately feeds the result)
    * @tparam A
    * @return a publisher (presumably) of type Pub of AppendStatus messages
    */
  def asStatusPublisher[A, Pub[_]: AsPublisher](
    nodeId: NodeId,
    clusterSize: Int,
    logAppendSuccess: LogAppendSuccess,
    nodeInput: Pub[(RaftMessage[A], RaftNodeResult[A])]): Pub[AppendStatus] = {
    var currentStatus: AppendStatus = {
      val appendMap = Map[NodeId, AppendEntriesResponse](nodeId -> AppendEntriesResponse.ok(logAppendSuccess.firstIndex.term, logAppendSuccess.lastIndex.index))
      // the log entry is committed only if this is a single-node cluster
      val committed = if (clusterSize == 1) {
        logAppendSuccess.appendedCoords
      } else {
        Set.empty[LogCoords]
      }
      AppendStatus(logAppendSuccess, appendMap, committed, clusterSize)
    }

    val firstStatus = currentStatus

    val updates: Pub[AppendStatus] = nodeInput.collect {
      case (AddressedMessage(from, appendResponse: AppendEntriesResponse), leaderCommitResp: LeaderCommittedResult[A]) if logAppendSuccess.contains(appendResponse) =>
        currentStatus = currentStatus.withResult(from, appendResponse, leaderCommitResp.committed)
        currentStatus
    }

    // the input 'nodeInput' is an infinite stream (or should be) of messages from peers, so we need to ensure we put in a complete condition
    // unfortunately we have this weird closure over a 'canComplete' because we want the semantics of 'takeWhile plus the first element which returns false'
    (firstStatus +: updates).takeWhileIncludeLast(!_.isComplete)
  }
}
