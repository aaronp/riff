package riff.raft.node

import riff.raft.Term

import scala.language.implicitConversions

class RichNodeState[NodeKey, A](val nodeState: RaftNode[NodeKey, A]) {

  def currentTerm: Term = nodeState.persistentState.currentTerm
  def withRaftNode(newState: NodeState[NodeKey]) = {
    import nodeState._
    new RaftNode(persistentState, log, timers, cluster, newState, maxAppendSize)
  }

  def withTerm(t: Term) = {
    import nodeState._
    val ps = PersistentState.inMemory[NodeKey]().currentTerm = t

    new RaftNode(ps, log, timers, cluster, state, maxAppendSize)
  }

}
object RichNodeState {
  implicit def asRichState[NodeKey, A](nodeState: RaftNode[NodeKey, A]) = new RichNodeState(nodeState)
}
