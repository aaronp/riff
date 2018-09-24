package riff.raft.node
import riff.raft.log.{LogAppendResult, LogCoords, LogEntry, RaftLog}
import riff.raft.messages.{RaftResponse, _}
import riff.raft.timer.{RaftTimer, Timers}
import riff.raft.{Term, node}

object RaftNode {

  def inMemory[NodeKey, A](id: NodeKey, maxAppendSize: Int = 10)(implicit timer: RaftTimer[NodeKey]): RaftNode[NodeKey, A] = {
    new RaftNode[NodeKey, A](
      PersistentState.inMemory().cached(),
      RaftLog.inMemory[A](),
      new Timers[NodeKey](),
      RaftCluster[NodeKey](Nil),
      node.FollowerNodeState[NodeKey](id, None),
      maxAppendSize
    )
  }
}

/**
  * The place where the different pieces which represent a Raft Node come together -- the glue code.
  *
  * I've looked at this a few different ways, but ultimately found this abstraction here to be the most readable,
  * and follows most closely what's laid out in the raft spec.
  *
  * It's not too generic/abstracted, but quite openly just orchestrates the pieces/interactions of the inputs
  * into a raft node.
  *
  * @param persistentState the state used to track the currentTerm and vote state
  * @param log the RaftLog which stores the log entries
  * @param timers the timing apparatus for controlling timeouts
  * @param cluster this node's view of the cluster
  * @param initialState the initial role
  * @param maxAppendSize the maximum number of entries to send to a follower at a time when catching up the log
  * @tparam NodeKey the underlying node peer type, which may just be a String identifier, or a websocket, actor, etc.
  *                 so long as it has a meaningful hashCode/equals
  * @tparam A the log entry type
  */
class RaftNode[NodeKey, A](val persistentState: PersistentState[NodeKey],
                           val log: RaftLog[A],
                           val timers: Timers[NodeKey],
                           val cluster: RaftCluster[NodeKey],
                           initialState: NodeState[NodeKey],
                           val maxAppendSize: Int)
    extends RaftMessageHandler[NodeKey, A] {

  private var currentState: NodeState[NodeKey] = initialState

  /** This function may be used as a convenience to generate append requests for a
    *
    * @param data the data to append
    * @return either some append requests or an error output if we are not the leader
    */
  def createAppend(data: Array[A]): RaftNodeResult[NodeKey, A] = {
    appendIfLeader(data) match {
      case None =>
        val leaderMsg = currentState.leader.fold("")(name => s". The leader is ${name}")
        NoOpResult(s"Can't create an append request as we are ${currentState.role} in term ${thisTerm}$leaderMsg")
      case Some((_, requests)) => requests
    }
  }

  /**
    * Exposes this as a means for generating an AddressedRequest of messages together with the append result
    * from the leader's log
    *
    * @param data the data to append
    * @return the append result coupled w/ the append request to send if this node is the leader
    */
  def appendIfLeader(data: Array[A]): Option[(LogAppendResult, AddressedRequest[NodeKey, A])] = {
    currentState match {
      case leader: LeaderNodeState[NodeKey] =>
        val res = leader.makeAppendEntries[A](log, thisTerm(), data)
        Option(res)
      case _ => None
    }
  }

  /**
    * Applies requests and responses coming to the node state and replies w/ any resulting messages
    *
    * @param from the node from which this message is received
    * @param msg the Raft message
    * @return and resulting messages (requests or responses)
    */
  def onMessage(from: NodeKey, msg: RequestOrResponse[NodeKey, A]): Result = {
    msg match {
      case request: RaftRequest[A] => AddressedResponse(from, onRequest(from, request))
      case reply: RaftResponse     => onResponse(from, reply)
    }
  }

  /**
    * Handle a response coming from 'from'
    *
    * @param from the originating node
    * @param reply the response
    * @return any messages resulting from having processed this response
    */
  def onResponse(from: NodeKey, reply: RaftResponse): Result = {
    reply match {
      case voteResponse: RequestVoteResponse => onRequestVoteResponse(from, voteResponse)
      case appendResponse: AppendEntriesResponse =>
        val (_, result) = onAppendEntriesResponse(from, appendResponse)
        result
    }
  }

  def onRequestVoteResponse(from: NodeKey, voteResponse: RequestVoteResponse): Result = {
    currentState match {
      case candidate: CandidateNodeState[NodeKey] =>
        currentState = candidate.onRequestVoteResponse(from, cluster, voteResponse)

        if (currentState.isLeader) {
          // notify leader change
          onBecomeLeader(currentState)
        } else {
          NoOpResult(s"Got vote ${voteResponse}, vote state is now : ${candidate.candidateState()}")
        }
      case _ =>
        NoOpResult(s"Got vote ${voteResponse} while in role ${currentState.role}, term ${thisTerm}")
    }
  }

  /**
    * We're either the leader and should update our peer view/commit log, or aren't and should ignore it
    *
    * @param appendResponse
    * @return the result
    */
  def onAppendEntriesResponse(from: NodeKey, appendResponse: AppendEntriesResponse): (Seq[LogCoords], Result) = {
    currentState match {
      case leader: LeaderNodeState[NodeKey] =>
        leader.onAppendResponse(from, log, persistentState.currentTerm, appendResponse, maxAppendSize)
      case _ =>
        val result = NoOpResult(s"Ignoring append response from $from as we're in role ${currentState.role}, term ${thisTerm}")
        (Nil, result)

    }
  }

  def onTimerMessage(timeout: TimerMessage): Result = {
    timeout match {
      case ReceiveHeartbeatTimeout => onReceiveHeartbeatTimeout()
      case SendHeartbeatTimeout    => onSendHeartbeatTimeout()
    }
  }

  private def createAppendOnHeartbeatTimeout(leader: LeaderNodeState[NodeKey], toNode: NodeKey) = {
    leader.clusterView.stateForPeer(toNode) match {
      // we don't know what state this node is in - send our default heartbeat
      case None => makeDefaultHeartbeat()

      // we're at the beginning of the log - send some data
      case Some(Peer(1, 0)) => AppendEntries(LogCoords.Empty, thisTerm, log.latestCommit(), log.entriesFrom(1, maxAppendSize))

      // the state where we've not yet matched a peer, so we're trying to send ever-decreasing indices,
      // using the 'nextIndex' (which should be decrementing on each fail)
      case Some(Peer(nextIndex, 0)) =>
        log.coordsForIndex(nextIndex) match {
          // "This should never happen" (c)
          // potential error situation where we have a 'nextIndex' -- resend the default heartbeat
          case None           => makeDefaultHeartbeat()
          case Some(previous) => AppendEntries(previous, thisTerm, log.latestCommit(), Array.empty[LogEntry[A]])
        }

      // the normal success state where we send our expected previous coords based on the match index
      case Some(Peer(nextIndex, matchIndex)) =>
        log.coordsForIndex(matchIndex) match {
          case None =>
            // "This should never happen" (c)
            // potential error situation where we have a 'nextIndex' -- resend the default heartbeat
            makeDefaultHeartbeat()
          case Some(previous) => AppendEntries(previous, thisTerm, log.latestCommit(), log.entriesFrom(nextIndex, maxAppendSize))
        }
    }
  }

  def onSendHeartbeatTimeout(): Result = {
    currentState match {
      case leader: LeaderNodeState[NodeKey] =>
        timers.sendHeartbeat.reset(nodeKey)

        val msgs = cluster.peers.map { toNode =>
          val heartbeat = createAppendOnHeartbeatTimeout(leader, toNode)
          toNode -> heartbeat
        }

        AddressedRequest(msgs)
      case _ =>
        NoOpResult(s"Received send heartbeat timeout, but we're ${currentState.role} in term ${thisTerm}")
    }
  }

  private def makeDefaultHeartbeat() = AppendEntries(log.latestAppended(), thisTerm, log.latestCommit(), Array.empty[LogEntry[A]])

  def onReceiveHeartbeatTimeout(): Result = {
    onBecomeCandidateOrLeader()
  }

  def onRequest(from: NodeKey, request: RaftRequest[A]): RaftResponse = {
    request match {
      case append: AppendEntries[A] => onAppendEntries(from, append)
      case vote: RequestVote        => onRequestVote(from, vote)
    }
  }

  def onAppendEntries(from: NodeKey, append: AppendEntries[A]): AppendEntriesResponse = {
    val beforeTerm = thisTerm()
    val doAppend = if (beforeTerm < append.term) {
      onBecomeFollower(Option(from), append.term)
      false
    } else if (beforeTerm > append.term) {
      // ignore/fail if we get an append for an earlier term
      false
    } else {
      // we're supposedly the leader of this term ... ???
      currentState match {
        case _: LeaderNodeState[NodeKey] => false
        case _ =>
          timers.receiveHeartbeat.reset(nodeKey)
          true
      }
    }

    if (doAppend) {
      val result: AppendEntriesResponse = log.onAppend(thisTerm, append)
      if (result.success) {
        log.commit(append.commitIndex)
      }
      result
    } else {
      AppendEntriesResponse.fail(thisTerm())
    }
  }

  /**
    * Create a reply to the given vote request.
    *
    * NOTE: Whatever the actual node 'A' is, it is expected that, upon a successful reply,
    * it updates it's own term and writes down (remembers) that it voted in this term so
    * as not to double-vote should this node crash.
    *
    * @param forRequest the data from the vote request
    * @return the RequestVoteResponse
    */
  def onRequestVote(from: NodeKey, forRequest: RequestVote): RequestVoteResponse = {
    val beforeTerm = persistentState.currentTerm
    val reply      = persistentState.castVote(log.latestAppended(), from, forRequest)

    // regardless of granting the vote or not, if we just saw a later term, we need to be a follower
    // ... and if we are (soon to be were) leader, we have to transition (cancel heartbeats, etc)
    if (beforeTerm < reply.term) {
      onBecomeFollower(None, reply.term)
    }
    reply
  }

  def onBecomeCandidateOrLeader(): AddressedRequest[NodeKey, A] = {
    val newTerm = thisTerm + 1
    persistentState.currentTerm = newTerm

    // write down that we're voting for ourselves
    persistentState.castVote(newTerm, nodeKey)

    // this election may end up being a split-brain, or we may have been disconnected. At any rate,
    // we need to reset our heartbeat timeout
    timers.receiveHeartbeat.reset(nodeKey)

    cluster.numberOfPeers match {
      case 0 =>
        val leader = currentState.becomeLeader(cluster)
        currentState = leader
        onBecomeLeader(leader)
      case clusterSize =>
        currentState = currentState.becomeCandidate(newTerm, clusterSize + 1)
        val requestVote = RequestVote(newTerm, log.latestAppended())
        AddressedRequest(cluster.peers.map(_ -> requestVote))
    }
  }

  def onBecomeFollower(newLeader: Option[NodeKey], newTerm: Term) = {
    if (currentState.isLeader) {
      // cancel HB for all nodes
      timers.sendHeartbeat.cancel(nodeKey)
    }
    timers.receiveHeartbeat.reset(nodeKey)
    persistentState.currentTerm = newTerm
    currentState = currentState.becomeFollower(newLeader)
  }

  def onBecomeLeader(state: NodeState[NodeKey]): AddressedRequest[NodeKey, A] = {
    val hb = makeDefaultHeartbeat()
    timers.receiveHeartbeat.cancel(nodeKey)
    timers.sendHeartbeat.reset(nodeKey)

    AddressedRequest(cluster.peers.map(_ -> hb))
  }

  def nodeKey: NodeKey = currentState.id

  def state(): NodeState[NodeKey] = currentState

  protected def thisTerm() = persistentState.currentTerm

  /** a convenience builder method to create a new raft node w/ the given raft log
    *
    * @return a new node state
    */
  def withLog(newLog: RaftLog[A]): RaftNode[NodeKey, A] = {
    new RaftNode(persistentState, newLog, timers, cluster, state, maxAppendSize)
  }

  /** a convenience builder method to create a new raft node w/ the given cluster
    *
    * @return a new node state
    */
  def withCluster(newCluster: RaftCluster[NodeKey]): RaftNode[NodeKey, A] = {
    new RaftNode(persistentState, log, timers, newCluster, state, maxAppendSize)
  }

  override def toString() = {
    s"""NodeState ${currentState.id} {
       |  timers : $timers
       |  cluster : $cluster
       |  currentState : $currentState
       |  log : ${log}
       |}""".stripMargin
  }

}
