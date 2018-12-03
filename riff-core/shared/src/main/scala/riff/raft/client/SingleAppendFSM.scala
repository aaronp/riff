package riff.raft.client

import riff.raft.log.{LogAppendSuccess, LogCoords}
import riff.raft.messages.AppendEntriesResponse
import riff.raft.{AppendOccurredOnDisconnectedLeader, AppendStatus, NodeId}

/**
  * This finite state machine represents states which can occur while a single append request's data is acked, committed, etc.
  * It contains the logic needed to support observing a stream of [[AppendStatus]] events associated with appending some data.
  *
  * This is done by combining input events from :
  *
  * 1) the AppendResponse messages coming into the node which processed the append
  * 2) a receiving node's log append results - both the initial one from the newly appended data, as well as all future appends.
  *    This is needed to see if any new appends result in needed to roll back our append, e.g. in the situation where we've appended
  *    while a disconnected leader and a new leader w/o that data has won an election.
  * 3) the node's log's commits, so we can add that information to the [[AppendStatus]]
  *
  * This state machine is updated via [[StateUpdateMsg]] messages.
  *
  * The states are:
  *
  * initial
  *
  */
sealed trait SingleAppendFSM {

  /**
    * Update the state-machine w/ the StateUpdateMsg
    *
    * @param msg the update message
    * @return an updated state
    */
  def update(msg: StateUpdateMsg): SingleAppendFSM

}

object SingleAppendFSM {

  def apply(nodeId: NodeId, clusterSize: Int): SingleAppendFSM = InitialState(nodeId, clusterSize)
}

case class ErrorState(err: Exception) extends SingleAppendFSM {
  override def update(msg: StateUpdateMsg): SingleAppendFSM = this
}

case class EarlyTerminationOnErrorState(status: AppendStatus) extends SingleAppendFSM {
  override def update(msg: StateUpdateMsg): SingleAppendFSM = this
}

case class InitialState(nodeId: NodeId, clusterSize: Int) extends SingleAppendFSM {

  override def update(msg: StateUpdateMsg): SingleAppendFSM = {
    msg match {
      case InitialLogAppend(logAppendSuccess: LogAppendSuccess) =>
        // the initial append, if successful, means we're the leader and our log has just appended some data.
        // we have this info, and have yet to send it out to other nodes
        val firstStatus = {
          val ourOwnAck = AppendEntriesResponse.ok(logAppendSuccess.firstIndex.term, logAppendSuccess.lastIndex.index)
          val appendMap = Map[NodeId, AppendEntriesResponse](nodeId -> ourOwnAck)
          val committedCoords = if (clusterSize == 1) {
            logAppendSuccess.appendedCoords
          } else {
            Set.empty[LogCoords]
          }
          AppendStatus(logAppendSuccess, appendMap, logAppendSuccess.appendedCoords, clusterSize, committedCoords)
        }
        new FilteringState(firstStatus, Option(firstStatus))
      case InitialLogAppend(err: Exception) => new ErrorState(err)
      case _                                =>
        // any other messages (log commit messages, append responses, etc) we can ignore until we're handed the initial
        // log append result
        this
    }
  }
}

/**
  * We're actively updating an append status based on inputs from committed logs, log appends and node responses
  *
  * @param status
  */
case class FilteringState(status: AppendStatus, nextUpdate: Option[AppendStatus]) extends SingleAppendFSM {

  private def stay() = {
    if (nextUpdate.isEmpty) {
      this
    } else {
      copy(nextUpdate = None)
    }
  }

  private def emit(newStatus: AppendStatus) = {
    FilteringState(newStatus, Option(newStatus))
  }

  private val appendedCoords = status.appendedCoords

  private def weAcceptedWhileDisconnected(someLogAppendResult: LogAppendSuccess) = {
    someLogAppendResult.replacedLogCoords.exists(appendedCoords.contains)
  }
  override def update(msg: StateUpdateMsg): SingleAppendFSM = {
    msg match {
      case InitialLogAppend(appendResult) =>
        new ErrorState(new IllegalStateException(s"We've received multiple InitialLogAppend messages while populating $status, the second being $appendResult"))
      case EntryCommitted(coords) =>
        if (appendedCoords.contains(coords)) {
          emit(status.withCommit(coords))
        } else {
          stay()
        }

      case NodeResponded(responseFrom: NodeId, appendResponse: AppendEntriesResponse) =>
        if (status.leaderAppendResult.contains(appendResponse)) {
          emit(status.withResult(responseFrom, appendResponse))
        } else {
          stay()
        }

      // we watch out log appends to see if either we get an error (e.g. if somebody else tries to commit, but we're no longer the leader)
      //
      // oooh - that's a point. We could append, send the acks, they're all on their way back, but then another leader election happens
      // and we get a subsequent append error due to not being the leader ... we don't necessarily want to complete the stream in error,
      // because it could all succeed.
      //
      // On the other hand, we don't want to just hang around for some arbitrary amount of time waiting for ACKs which may not even be on their way.
      //
      // I guess the right thing to do would be to add this information to the AppendStatus itself and complete the stream, not in error, but w/ this exception/info
      //
      case LogAppend(someLogAppendResult: LogAppendSuccess) =>
        if (weAcceptedWhileDisconnected(someLogAppendResult)) {
          val err = new AppendOccurredOnDisconnectedLeader(status.leaderAppendResult, someLogAppendResult)
          EarlyTerminationOnErrorState(status.copy(errorAfterAppend = Option(err)))
        } else {
          this
        }
      case LogAppend(err: Exception) => EarlyTerminationOnErrorState(status.copy(errorAfterAppend = Option(err)))
    }
  }
}
