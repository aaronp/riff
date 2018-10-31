package riff.raft.node

import riff.raft.log.LogCoords
import riff.raft.messages.{RequestVote, RequestVoteResponse}
import riff.raft.node.PersistentState.CachedPersistentState
import riff.raft.{NodeId, Term}

/**
  * Keeps track of a node's internal state:
  * 1) what term it is
  * 2) who the chuff it voted for
  */
trait PersistentState {

  /** @param term
    * @return the recipient of the vote for the given term
    */
  def votedFor(term: Term): Option[String]

  /** @param term the term for which we're casting our vote
    * @param node the key of the cluster node for which we're voting
    */
  def castVote(term: Term, node: NodeId): Unit

  def currentTerm: Term

  def currentTerm_=(term: Term): PersistentState

  def hasAlreadyVotedInTerm(term: Term): Boolean = votedFor(term).isDefined

  /** convenience call to ensure the currentTerm calls are cached
    *
    * @return a caching persistent state
    */
  def cached(): PersistentState = this match {
    case c: CachedPersistentState => c
    case other                    => new CachedPersistentState(other)
  }

  /**
    * cast a vote given the request and optionally update our 'votedFor' state if it's the first time we've voted
    * for the term
    *
    * @param ourLogState the most recent append coordinates from our log
    * @param requestingNodeId the requesting node ID
    * @param forRequest the vote request to which we're asked to reply
    * @tparam NodeKey
    * @tparam A
    * @return the vote response
    */
  def castVote(latestAppendedLogEntry: => LogCoords,
               requestingNodeId: NodeId,
               forRequest: RequestVote): RequestVoteResponse = {
    def logStateOk = {
      val ourLogState = latestAppendedLogEntry
      forRequest.lastLogTerm >= ourLogState.term &&
      forRequest.lastLogIndex >= ourLogState.index
    }

    val ourTerm = currentTerm
    val granted: Boolean = {
      forRequest.term >= ourTerm &&
      !hasAlreadyVotedInTerm(forRequest.term) &&
      logStateOk
    }

    val replyTerm = if (granted) {
      castVote(forRequest.term, requestingNodeId)
      currentTerm = forRequest.term
      forRequest.term
    } else {
      if (forRequest.term > ourTerm) {
        currentTerm = forRequest.term
        forRequest.term
      } else {
        ourTerm
      }
    }
    RequestVoteResponse(replyTerm, granted)
  }
}

object PersistentState {

  def inMemory() = new InMemoryPersistentState

  class CachedPersistentState(underlying: PersistentState) extends PersistentState {
    private var cachedTerm                                = Option.empty[Term]
    override def votedFor(term: Term): Option[NodeId]     = underlying.votedFor(term)
    override def castVote(term: Term, node: NodeId): Unit = underlying.castVote(term, node)
    override def currentTerm: Term = {
      cachedTerm.getOrElse {
        val t = underlying.currentTerm
        cachedTerm = Option(t)
        t
      }
    }
    override def currentTerm_=(term: Term) = {
      if (!cachedTerm.exists(_ == term)) {
        underlying.currentTerm_=(term)
        cachedTerm = Option(term)
      }
      this
    }
  }

  /**
    * A purely in-memory "persistent" state for testing.
    *
    * In production this should NEVER be used as we need to guarantee that we don't ever dual-vote in a term.
    * It is important that we cater for the case where we:
    *
    * 1) cast a vote
    * 2) die
    * 3) recover
    * 4) receive another vote request for the same term as #1
    *
    * That we don't case two votes
    *
    * @tparam NodeKey
    */
  class InMemoryPersistentState extends PersistentState {
    private var votedForByTerm                        = Map[Term, NodeId]()
    private var term                                  = 0
    override def votedFor(term: Term): Option[NodeId] = votedForByTerm.get(term)
    override def castVote(term: Term, node: NodeId): Unit = {
      require(!votedForByTerm.contains(term), s"already voted in term $term for ${votedForByTerm(term)}")
      votedForByTerm = votedForByTerm.updated(term, node)
    }
    override def currentTerm: Term = term
    override def currentTerm_=(newTerm: Term): PersistentState = {
      require(newTerm >= term, s"Attempt to set term '${term}' to a previous term '$newTerm'")
      term = newTerm
      this
    }
  }
}
