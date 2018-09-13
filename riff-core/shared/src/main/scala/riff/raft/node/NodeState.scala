package riff.raft.node
import riff.raft.log.{LogAppendResult, LogCoords, LogEntry, RaftLog}
import riff.raft.messages.{AppendEntries, AppendEntriesResponse, RequestVoteResponse}
import riff.raft.{Term, isMajority}

import scala.collection.immutable

/**
  * Represents an in-memory state matching a node's role
  *
  * @tparam NodeKey
  */
sealed trait NodeState[NodeKey] {
  def id: NodeKey

  def becomeFollower(leader: Option[NodeKey]): FollowerNodeState[NodeKey] = FollowerNodeState(id, leader = leader)

  def becomeCandidate(term: Term, clusterSize: Int): CandidateNodeState[NodeKey] = {
    new CandidateNodeState[NodeKey](id, new CandidateState[NodeKey](term, Set(id), Set.empty, clusterSize))
  }

  def becomeLeader(cluster: RaftCluster[NodeKey]): LeaderNodeState[NodeKey] = LeaderNodeState(id, cluster)

  def leader: Option[NodeKey]
  def role: NodeRole
  def isFollower: Boolean  = role == Follower
  def isLeader: Boolean    = role == Leader
  def isCandidate: Boolean = role == Candidate

  def asLeader: Option[LeaderNodeState[NodeKey]] = this match {
    case leader: LeaderNodeState[NodeKey] => Option(leader)
    case _                           => None
  }
}

final case class FollowerNodeState[NodeKey](override val id: NodeKey, override val leader: Option[NodeKey]) extends NodeState[NodeKey] {
  override val role = Follower
}

final case class CandidateNodeState[NodeKey](override val id: NodeKey, initialState: CandidateState[NodeKey]) extends NodeState[NodeKey] {
  override val role = Candidate
  private var voteResponses = initialState
  override val leader: Option[NodeKey] = None
  def candidateState() = voteResponses

  /**
    * @param from the node which sent this response
    * @param cluster the raft cluster, should we be able to become the leader
    * @param voteResponse the vote response
    * @return a raft node -- either the candidate or a leader
    */
  def onRequestVoteResponse(from: NodeKey, cluster: RaftCluster[NodeKey], voteResponse: RequestVoteResponse): NodeState[NodeKey] = {
    voteResponses = voteResponses.update(from, voteResponse)
    if (voteResponses.canBecomeLeader) {
      LeaderNodeState(id, cluster)
    } else {
      this
    }
  }
}

final case class LeaderNodeState[NodeKey](override val id: NodeKey, clusterView: LeadersClusterView[NodeKey]) extends NodeState[NodeKey] {
  override def leader = Option(id)

  /** appends the data to the given leader's log, returning the append result and append requests based on the clusterView
    *
    * @param log the leader's log from which the previous append/commit are taken
    * @param currentTerm the leader node's term
    * @param data the data to append
    * @tparam A
    * @return the result from appending the entries to the leader's log, as well as an addressed request
    */
  def makeAppendEntries[A](log: RaftLog[A], currentTerm: Term, data: Array[A]): (LogAppendResult, AddressedRequest[NodeKey, A]) = {
    val previous: LogCoords         = log.latestAppended()
    val entries: Array[LogEntry[A]] = data.map(LogEntry(currentTerm, _))
    val appendResult = log.appendAll(previous.index + 1, entries)

    val requests: immutable.Iterable[(NodeKey, AppendEntries[A])] = {
      val peersWithMatchingLogs = clusterView.eligibleNodesForPreviousEntry(previous)

      //
      // check if we're a single-node cluster, in which case we can commit immediately as the leader
      //
      if (peersWithMatchingLogs.isEmpty) {
        if (clusterView.numberOfPeers() == 0) {
          log.commit(log.latestAppended().index)
        }
        Nil
      } else {
        val request = AppendEntries[A](previous, currentTerm, log.latestCommit(), entries)
        peersWithMatchingLogs.map(_ -> request)
      }
    }
    appendResult -> AddressedRequest(requests)
  }

  override val role = Leader

  def clusterSize = clusterView.numberOfPeers + 1

  /**
    * Handle the append response coming from the 'from' node
    *
    * @param from the node replying, presumably to some sent append request
    * @param log the leader's log
    * @param currentTerm the leader's current term
    * @param appendResponse the response which we're applying
    * @param maxAppendSize the maximum number of subsequent entries we'll send should we be trying to catch the 'from' node up
    * @tparam A the log type
    * @return the committed log coords resulting from having applied this response and the state output (either a no-op or a subsequent [[AppendEntries]] request)
    */
  def onAppendResponse[A](from: NodeKey,
                          log: RaftLog[A],
                          currentTerm: Term,
                          appendResponse: AppendEntriesResponse,
                          maxAppendSize: Int): (Seq[LogCoords], RaftNodeResult[NodeKey, A]) = {

    val latestAppended = log.latestAppended()

    //
    // first update the cluster view - update or decrement the nextIndex, matchIndex
    //
    clusterView.update(from, appendResponse) match {
      case Some(newPeerState) if appendResponse.success =>
        val values = log.entriesFrom(newPeerState.nextIndex, maxAppendSize)

        // if the majority of the peers have the same match index
        val count = clusterView.matchIndexCount(appendResponse.matchIndex) + 1

        val committed: Seq[LogCoords] = if (isMajority(count, clusterSize)) {
          log.commit(appendResponse.matchIndex)
        } else {
          Nil
        }

        // if there are log entries which occur 'after' this entry, they should then be sent next
        val reply = if (latestAppended.index > appendResponse.matchIndex) {
          log.coordsForIndex(appendResponse.matchIndex) match {
            case Some(previous) =>
              AddressedRequest(from, AppendEntries(previous, currentTerm, log.latestCommit(), values))
            case None =>
              NoOpResult(s"Couldn't read the log entry at ${appendResponse.matchIndex}. The latest append is ${log.latestAppended()}")
          }
        } else {
          NoOpResult("The leader thanks you for your reply - you're all up-to-date!")
        }
        (committed, reply)
      case _ =>
        // try again w/ an older index. This trusts that the cluster does the right thing when updating its peer view
        val reply = clusterView.stateForPeer(from) match {
          case Some(peer) =>
            val idx    = peer.nextIndex.min(latestAppended.index)
            val coords = log.coordsForIndex(idx).getOrElse(latestAppended)
            AddressedRequest(from, AppendEntries(coords, currentTerm, log.latestCommit(), Array.empty[LogEntry[A]]))
          case None =>
            NoOpResult(s"Couldn't find peer $from in the cluster(!), ignoring append entries response")
        }

        (Nil, reply)
    }
  }
}

object LeaderNodeState {
  def apply[NodeKey](id: NodeKey, cluster: RaftCluster[NodeKey]): LeaderNodeState[NodeKey] = {
    new LeaderNodeState[NodeKey](id, LeadersClusterView(cluster))
  }
}
