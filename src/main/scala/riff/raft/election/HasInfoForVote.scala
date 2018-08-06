package riff.raft.election
import riff.raft.log.LogCoords
import simulacrum.typeclass

/**
  * Represents the data a raft node would need to expose in order to evaluate an election request
  *
  * The rules for granting a vote are:
  * 1) A vote isn't already granted for the term
  * 2) the term is greater than the node's current term
  * 3) the log index is greater than this node's log length
  * 3) the log term is greater than or equal to this node's log term
  *
  * @tparam A the type of whatever representation of a raft node we have
  */
@typeclass trait HasInfoForVote[A] {

  /**
    * Create a reply to the given vote request.
    *
    * NOTE: Whatever the actual node 'A' is, it is expected that, upon a successful reply,
    * it updates it's own term and writes down (remembers) that it voted in this term so
    * as not to double-vote should this node crash.
    *
    * @param receivingRaftNode the raft node which is processing the vote request
    * @param forRequest the data from the vote request
    * @return the RequestVoteReply
    */
  final def replyTo(receivingRaftNode: A, forRequest: RequestVoteData): RequestVoteReply = {
    def logStateOk = {
      val ourLogState = logState(receivingRaftNode)
      forRequest.lastLogTerm >= ourLogState.term &&
      forRequest.lastLogIndex >= ourLogState.index
    }

    val ourTerm = currentTerm(receivingRaftNode)
    val granted: Boolean = {
      forRequest.term >= ourTerm &&
      !hasAlreadyVotedInTerm(receivingRaftNode, forRequest.term) &&
      logStateOk
    }

    if (granted) {
      RequestVoteReply(ourTerm + 1, granted)
    } else {
      RequestVoteReply(ourTerm, false)
    }
  }

  /** @param raftNode the raft node which is processing the vote request
    * @return the current term of the node
    */
  def currentTerm(raftNode: A): Int

  /** @param raftNode the raft node which is processing the vote request
    * @param term the term we're voting in
    * @return true if the raft node has already voted in an election for this term
    */
  def hasAlreadyVotedInTerm(raftNode: A, term: Int): Boolean

  /** @param raftNode the raft node which is processing the vote request
    * @return the current state of our log
    */
  def logState(raftNode: A): LogCoords
}

object HasInfoForVote {

  /** @param currentTermValue the current term
    * @param logStateValue the current log state
    * @param votedInTerm the function used to determine if it voted in the given term
    * @tparam T the type for this HasInfoForVote
    * @return a HasInfoForVote for any type T
    */
  def const[A](term: Int, log: LogCoords)(votedInTerm: Int => Boolean) = new HasInfoForVote[A] {
    override def currentTerm(ignored: A): Int = term

    override def hasAlreadyVotedInTerm(ignored: A, term: Int): Boolean = votedInTerm(term)

    override def logState(ignored: A): LogCoords = log
  }
}
