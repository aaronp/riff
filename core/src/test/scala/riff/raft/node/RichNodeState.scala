package riff.raft.node
import riff.raft.Term
import riff.raft.log.RaftLog

class RichNodeState[NodeKey, A](val nodeState: NodeState[NodeKey, A]) {

  /**
    * copy was added as a convenience for setting up the node state (e.g. builder pattern), but is NOT intended to be used once messages
    * are being piped through.
    *
    * @return a new node state
    */
  def withLog(newLog: RaftLog[A]): NodeState[NodeKey, A] = {
    import nodeState._
    new NodeState(persistentState, newLog, timers, cluster, raftNode, maxAppendSize)
  }

  def currentTerm = nodeState.persistentState.currentTerm

  def withCluster(newCluster: RaftCluster[NodeKey]) = {
    import nodeState._
    new NodeState(persistentState, log, timers, newCluster, raftNode, maxAppendSize)
  }
  def withRaftNode(newState: RaftNode[NodeKey]) = {
    import nodeState._
    new NodeState(persistentState, log, timers, cluster, newState, maxAppendSize)
  }

  def withTerm(t: Term) = {
    import nodeState._
    val ps = PersistentState.inMemory[NodeKey]().currentTerm = t

    new NodeState(ps, log, timers, cluster, raftNode, maxAppendSize)
  }

}
object RichNodeState {
  implicit def asRichState[NodeKey, A](nodeState: NodeState[NodeKey, A]) = new RichNodeState(nodeState)
}
