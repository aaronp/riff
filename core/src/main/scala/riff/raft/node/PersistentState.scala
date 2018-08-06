package riff.raft.node

import java.nio.file.Path

import eie.io._
import riff.raft.Term
import riff.raft.log.LogCoords
import riff.raft.messages.{RequestVote, RequestVoteResponse}
import riff.raft.node.PersistentState.CachedPersistentState

/**
  * Keeps track of a node's internal state:
  * 1) what term it is
  * 2) who the chuff it voted for
  */
trait PersistentState[NodeKey] {

  /** @param term
    * @return the recipient of the vote for the given term
    */
  def votedFor(term: Term): Option[NodeKey]

  /** @param term the term for which we're casting our vote
    * @param node the key of the cluster node for which we're voting
    */
  def castVote(term: Term, node: NodeKey): Unit

  def currentTerm: Term

  def currentTerm_=(term: Term): PersistentState[NodeKey]

  def hasAlreadyVotedInTerm(term: Term): Boolean = votedFor(term).isDefined

  /** convenience call to ensure the currentTerm calls are cached
    *
    * @return a caching persistent state
    */
  def cached(): PersistentState[NodeKey] = this match {
    case c: CachedPersistentState[NodeKey] => c
    case other                             => new CachedPersistentState[NodeKey](other)
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
  def castVote(latestAppendedLogEntry: => LogCoords, requestingNodeId: NodeKey, forRequest: RequestVote): RequestVoteResponse = {
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

  def apply[A: ToBytes: FromBytes](dir: Path, createDirIfNotExists: Boolean): PersistentState[A] = {
    val dirOk = dir.isDir || (createDirIfNotExists && dir.mkDirs().isDir)
    require(dirOk, s"${dir} is not a directory")
    new FileBasedPersistentState[A](dir).cached()
  }

  def inMemory[NodeKey]() = new InMemoryPersistentState[NodeKey]

  class CachedPersistentState[NodeKey](underlying: PersistentState[NodeKey]) extends PersistentState[NodeKey] {
    private var cachedTerm                                 = Option.empty[Term]
    override def votedFor(term: Term): Option[NodeKey]     = underlying.votedFor(term)
    override def castVote(term: Term, node: NodeKey): Unit = underlying.castVote(term, node)
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
  class InMemoryPersistentState[NodeKey] extends PersistentState[NodeKey] {
    private var votedForByTerm                         = Map[Term, NodeKey]()
    private var term                                   = 0
    override def votedFor(term: Term): Option[NodeKey] = votedForByTerm.get(term)
    override def castVote(term: Term, node: NodeKey): Unit = {
      require(!votedForByTerm.contains(term), s"already voted in term $term")
      votedForByTerm = votedForByTerm.updated(term, node)
    }
    override def currentTerm: Term = term
    override def currentTerm_=(newTerm: Term): PersistentState[NodeKey] = {
      require(newTerm >= term)
      term = newTerm
      this
    }
  }

  class FileBasedPersistentState[NodeKey: ToBytes: FromBytes](dir: Path) extends PersistentState[NodeKey] {

    private val currentTermFile = {
      val path = dir.resolve(".currentTerm")
      if (!path.exists()) {
        path.text = 0.toString
      }
      path
    }

    override def currentTerm(): Term = currentTermFile.text.toInt

    override def currentTerm_=(term: Term) = {
      val current = currentTerm()
      require(term > current, s"attempt to decrement term from $current to $term")
      currentTermFile.text = term.toString
      this
    }

    private def votedForFile(term: Term) = dir.resolve(s"${term}.votedFor")

    override def votedFor(term: Term): Option[NodeKey] = {
      val path = votedForFile(term)
      if (path.exists()) {
        FromBytes[NodeKey].read(path.bytes).toOption
      } else {
        None
      }
    }

    override def castVote(term: Term, node: NodeKey) = {
      val alreadyVoted = votedFor(term)
      require(alreadyVoted.isEmpty, s"Already voted in term $term for $alreadyVoted")
      votedForFile(term).bytes = ToBytes[NodeKey].bytes(node)
    }

  }

}
