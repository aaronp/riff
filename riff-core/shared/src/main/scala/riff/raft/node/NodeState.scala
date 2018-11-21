package riff.raft.node
import riff.raft.log.{LogCoords, LogEntry, RaftLog}
import riff.raft.messages.{AppendEntries, AppendEntriesResponse, RequestVoteResponse}
import riff.raft.{NodeId, Term, isMajority}

import scala.collection.immutable

/**
  * Represents an in-memory state matching a node's role
  *
  * @tparam NodeId
  */
sealed trait NodeState {
  def id: NodeId

  def becomeFollower(leader: Option[NodeId]): FollowerNodeState = FollowerNodeState(id, leader = leader)

  def becomeCandidate(term: Term, clusterSize: Int): CandidateNodeState = {
    new CandidateNodeState(id, new CandidateState(term, Set(id), Set.empty, clusterSize))
  }

  def becomeLeader(cluster: RaftCluster): LeaderNodeState = LeaderNodeState(id, cluster)

  def leader: Option[NodeId]
  def role: NodeRole
  def isFollower: Boolean  = role == Follower
  def isLeader: Boolean    = role == Leader
  def isCandidate: Boolean = role == Candidate

  def asLeader: Option[LeaderNodeState] = this match {
    case leader: LeaderNodeState => Option(leader)
    case _                       => None
  }
}

final case class FollowerNodeState(override val id: NodeId, override val leader: Option[NodeId]) extends NodeState {
  override val role = Follower
}

final case class CandidateNodeState(override val id: NodeId, initialState: CandidateState) extends NodeState {
  override val role                   = Candidate
  private var voteResponses           = initialState
  override val leader: Option[NodeId] = None
  def candidateState()                = voteResponses

  /**
    * @param from the node which sent this response
    * @param cluster the raft cluster, should we be able to become the leader
    * @param voteResponse the vote response
    * @return a raft node -- either the candidate or a leader
    */
  def onRequestVoteResponse(from: NodeId, cluster: RaftCluster, voteResponse: RequestVoteResponse): NodeState = {
    voteResponses = voteResponses.update(from, voteResponse)
    if (voteResponses.canBecomeLeader) {
      LeaderNodeState(id, cluster)
    } else {
      this
    }
  }
}

final case class LeaderNodeState(override val id: NodeId, clusterView: LeadersClusterView) extends NodeState {
  override def leader = Option(id)

  /** appends the data to the given leader's log, returning the append result and append requests based on the clusterView
    *
    * @param log the leader's log from which the previous append/commit are taken
    * @param currentTerm the leader node's term
    * @param data the data to append
    * @tparam A
    * @return the result from appending the entries to the leader's log, as well as an addressed request
    */
  def makeAppendEntries[A](log: RaftLog[A], currentTerm: Term, data: Array[A]): NodeAppendResult[A] = {
    val previous: LogCoords         = log.latestAppended()
    val entries: Array[LogEntry[A]] = data.map(LogEntry(currentTerm, _))
    val appendResult                = log.appendAll(previous.index + 1, entries)

    val requests: immutable.Iterable[(NodeId, AppendEntries[A])] = {
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
    NodeAppendResult(appendResult, AddressedRequest(requests))
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
  def onAppendResponse[A](from: NodeId, log: RaftLog[A], currentTerm: Term, appendResponse: AppendEntriesResponse, maxAppendSize: Int): LeaderCommittedResult[A] = {

    val latestAppended = log.latestAppended()

    //
    // it's the leader's responsibility to send the right commit index for the follower nodes, who should
    // blindly just commit what they're asked to. And so ... we mustn't send a commit index higher than the
    // index we've asked the node to append
    //
    def commitIdxForPeer(newPeerState : Peer, numAppended :Int) = {
      val highestSentIndexInclusive = newPeerState.nextIndex + numAppended - 1
      log.latestCommit().min(highestSentIndexInclusive)
    }

    //
    // first update the cluster view - update or decrement the nextIndex, matchIndex
    //
    val (committedCoords, result) = clusterView.update(from, appendResponse) match {
      case Some(newPeerState) if appendResponse.success =>
        val values: Array[LogEntry[A]] = log.entriesFrom(newPeerState.nextIndex, maxAppendSize)

        // if the majority of the peers have the same match index
        val count = clusterView.matchIndexCount(appendResponse.matchIndex) + 1

        // this logic will try to commit as soon as the majority is received, but only the first will return coords.
        // e.g. in a 5 node cluster, once we have the 2nd successful ack, we'll commit. When we get the 3rd and 4th,
        // this code will still invoke 'log.commit', but the log will return an empty result
        val committed: Seq[LogCoords] = if (isMajority(count, clusterSize)) {
          log.commit(appendResponse.matchIndex)
        } else {
          Nil
        }

        // if there are log entries which occur 'after' this entry, they should then be sent next
        val reply = if (latestAppended.index > appendResponse.matchIndex) {
          log.coordsForIndex(appendResponse.matchIndex) match {
            case Some(previous) =>
              val commitIdx = commitIdxForPeer(newPeerState, values.size)
              AddressedRequest(from, AppendEntries(previous, currentTerm, commitIdx, values))
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
            val idx: Term = peer.nextIndex.min(latestAppended.index)

            // if the next index is 1, we've reached the beginning of the log
            val entries = if (idx == 1) {
              val values = log.entriesFrom(idx, maxAppendSize)
              val commitIdx = commitIdxForPeer(peer, values.size)
              AppendEntries(LogCoords.Empty, currentTerm, commitIdx, values)
            } else {
              val prevCoords: LogCoords = log.coordsForIndex(idx).getOrElse(latestAppended)
              val commitIdx: Term       = log.latestCommit().min(prevCoords.index)
              AppendEntries(prevCoords, currentTerm, commitIdx, Array.empty[LogEntry[A]])
            }
            AddressedRequest(from, entries)
          case None =>
            NoOpResult(s"Couldn't find peer $from in the cluster(!), ignoring append entries response")
        }

        (Nil, reply)
    }

    LeaderCommittedResult(committedCoords, result)
  }
}

object LeaderNodeState {

  def apply(id: NodeId, cluster: RaftCluster): LeaderNodeState = {
    new LeaderNodeState(id, LeadersClusterView(cluster))
  }
}
