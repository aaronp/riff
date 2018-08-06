package riff.raft.node

import riff.raft.isMajority
import riff.raft.Term
import riff.raft.messages.RequestVoteResponse

/**
  * Contains the functions for when a node becomes a candidate due to a heartbeat timeout
  */
case class CandidateState[A](term: Term, votesFor: Set[A], votesAgainst: Set[A], clusterSize: Int) {

  def canBecomeLeader    = isMajority(votesFor.size, clusterSize)
  def cannotBecomeLeader = isMajority(votesAgainst.size, clusterSize)

  def update(from: A, reply: RequestVoteResponse): CandidateState[A] = {
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
