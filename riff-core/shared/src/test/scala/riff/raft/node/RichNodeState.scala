package riff.raft.node

import riff.raft.Term

import scala.language.implicitConversions

class RichNodeState[NodeKey, A](val nodeState: NodeState[NodeKey, A]) {

  def currentTerm: Term = nodeState.persistentState.currentTerm
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
