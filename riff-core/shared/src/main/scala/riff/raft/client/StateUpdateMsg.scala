package riff.raft.client

import riff.raft.NodeId
import riff.raft.log.{LogAppendResult, LogCoords}
import riff.raft.messages.AppendEntriesResponse

/**
  * Represents the fixed inputs to the [[SingleAppendFSM]] in order to support a feed of [[riff.raft.AppendStatus]] messages
  */
sealed trait StateUpdateMsg

object StateUpdateMsg {

  /** The log append which we know to be a result of inserting the data for which we want to observe
    *
    * @param appendResult the log append result of appending some new data
    * @return a StateUpdateMsg
    */
  def initialAppend(appendResult: LogAppendResult): InitialLogAppend = InitialLogAppend(appendResult)

  /** Create a message from a log commit result (any log commit result).
    *
    * We need this not onto to update our [[riff.raft.AppendStatus]] messages, but also to know if we should complete w/o having
    * had all the responses.
    *
    * We need to consider this scenario:
    *
    * 1) we're the leader of a 5 node cluster
    * 2) one of the followers goes down
    * 3) we accept an append and eventually commit it when ack'd by the other nodes (but obviously not by the down node)
    * 4) another node becomes leader
    * 5) the down node comes up
    * !!) the "client" observing our updates will never get an 'onComplete', because the down node will never response to
    *     us, the old leader
    *
    * In this situation, we can notice when our log is committed, and thereby end/complete the Observable[AppendStatus]
    *
    * @param committed the committed log result
    * @return a StateUpdateMsg
    */
  def logCommit(committed: LogCoords): EntryCommitted = EntryCommitted(committed)

  /** Create a message from a log appendresult (any log append result) - used to determine if our data is overwritten
    * by a new leader
    *
    * @param committed the append log result
    * @return a StateUpdateMsg
    */
  def logAppend(appendResult: LogAppendResult): LogAppend = LogAppend(appendResult)

  /** Create a message from a node's response message to this node
    *
    * @param responseFrom the sending node ID
    * @param appendResponse the response
    * @return a StateUpdateMsg
    */
  def responseFromNode(responseFrom: NodeId, appendResponse: AppendEntriesResponse) = NodeResponded(responseFrom, appendResponse)
}

case class EntryCommitted(committed: LogCoords)                                       extends StateUpdateMsg
case class NodeResponded(responseFrom: NodeId, appendResponse: AppendEntriesResponse) extends StateUpdateMsg
case class InitialLogAppend(appendResult: LogAppendResult)                            extends StateUpdateMsg
case class LogAppend(appendResult: LogAppendResult)                                   extends StateUpdateMsg
