package riff.raft.node

import riff.raft.{NodeId, Term, isMajority}
import riff.raft.messages.RequestVoteResponse

/**
  * Contains the functions for when a node becomes a candidate due to a heartbeat timeout
  */
case class CandidateState(term: Term, votesFor: Set[NodeId], votesAgainst: Set[NodeId], clusterSize: Int) {

  def canBecomeLeader    = isMajority(votesFor.size, clusterSize)

  def update(from: NodeId, reply: RequestVoteResponse): CandidateState = {
    if (reply.term == term) {
      if (reply.granted) {
        copy(votesFor = votesFor + from)
      } else {
        copy(votesAgainst = votesAgainst + from)
      }
    } else {
      this
    }
  }
}
