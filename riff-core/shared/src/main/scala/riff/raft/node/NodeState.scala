package riff.raft.node
import riff.raft.log.{LogCoords, LogEntry, RaftLog}
import riff.raft.messages.{RaftResponse, _}
import riff.raft.timer.{RaftTimer, Timers}
import riff.raft.{Term, node}

object NodeState {

  def inMemory[NodeKey, A](id: NodeKey, maxAppendSize: Int = 10)(implicit timer: RaftTimer[NodeKey]): NodeState[NodeKey, A] = {
    new NodeState[NodeKey, A](
      PersistentState.inMemory().cached(),
      RaftLog.inMemory[A](),
      new Timers[NodeKey](),
      RaftCluster[NodeKey](Nil),
      node.FollowerNode[NodeKey](id, None),
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
class NodeState[NodeKey, A](val persistentState: PersistentState[NodeKey],
                            val log: RaftLog[A],
                            val timers: Timers[NodeKey],
                            val cluster: RaftCluster[NodeKey],
                            initialState: RaftNode[NodeKey],
                            val maxAppendSize: Int)
    extends RaftMessageHandler[NodeKey, A] {

  override def toString() = {
    s"""RaftNode ${currentState.id} {
       |  timers : $timers
       |  cluster : $cluster
       |  currentState : $currentState
       |  log : ${log}
       |}""".stripMargin
  }

  private var currentState: RaftNode[NodeKey] = initialState

  def nodeKey: NodeKey = currentState.id

  def raftNode(): RaftNode[NodeKey] = currentState

  protected def thisTerm() = persistentState.currentTerm

  /** This function may be used as a convenience to generate append requests for a
    *
    * @param data the data to append
    * @return either some append requests or an error output if we are not the leader
    */
  def createAppend(data: Array[A]): NodeStateOutput[NodeKey, A] = {
    withLeader { leader =>
      val (_, requests) = leader.makeAppendEntries[A](log, thisTerm(), data)
      requests.nodes.foreach(timers.sendHeartbeat.reset)

      requests
    }
  }

  private def withLeader(f: LeaderNode[NodeKey] => AddressedRequest[NodeKey, A]) = {
    currentState match {
      case leader: LeaderNode[NodeKey] => f(leader)
      case _ =>
        val leaderMsg = currentState.leader.fold("")(name => s". The leader is ${name}")
        NoOpOutput(s"Can't create an append request as we are ${currentState.role} in term ${thisTerm}$leaderMsg")
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
      case candidate: CandidateNode[NodeKey] =>
        currentState = candidate.onRequestVoteResponse(from, cluster, log.latestAppended(), voteResponse)

        if (currentState.isLeader) {
          // notify leader change
          onBecomeLeader(currentState)
        } else {
          NoOpOutput(s"Got vote ${voteResponse}, vote state is now : ${candidate.candidateState()}")
        }
      case _ =>
        NoOpOutput(s"Got vote ${voteResponse} while in role ${currentState.role}, term ${thisTerm}")
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
      case leader: LeaderNode[NodeKey] =>
        leader.onAppendResponse(from, log, persistentState.currentTerm, appendResponse, maxAppendSize)
      case _ =>
        val result = NoOpOutput(s"Ignoring append response from $from as we're in role ${currentState.role}, term ${thisTerm}")
        (Nil, result)

    }
  }

  def onTimerMessage(timeout: TimerMessage[NodeKey]): Result = {
    timeout match {
      case ReceiveHeartbeatTimeout      => onReceiveHeartbeatTimeout()
      case SendHeartbeatTimeout(toNode) => onSendHeartbeatTimeout(toNode)
    }
  }

  private def createAppendOnHeartbeatTimeout(leader: LeaderNode[NodeKey], toNode: NodeKey) = {
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

  def onSendHeartbeatTimeout(toNode: NodeKey): Result = {
    currentState match {
      case leader: LeaderNode[NodeKey] =>
        timers.sendHeartbeat.reset(toNode)

        val heartbeat = createAppendOnHeartbeatTimeout(leader, toNode)
        AddressedRequest(toNode -> heartbeat)

      case _ =>
        NoOpOutput(s"Received send heartbeat timeout for $toNode, but we're ${currentState.role} in term ${thisTerm}")
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

  def onAppendEntries(from: NodeKey, append: AppendEntries[A]): RaftResponse = {
    val becameFollower = if (thisTerm < append.term) {
      onBecomeFollower(Option(from), append.term)
      true
    } else {
      false
    }

    currentState match {
      case _: LeaderNode[NodeKey] => AppendEntriesResponse.fail(thisTerm())
      case _ =>
        if (!becameFollower) {
          timers.receiveHeartbeat.reset(currentState.id)
        }
        log.onAppend(thisTerm, append)
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

    cluster.size + 1 match {
      case 1 =>
        val leader: LeaderNode[NodeKey] = currentState.becomeLeader(cluster, log.latestAppended())
        currentState = leader
        onBecomeLeader(leader)
      case clusterSize =>
        currentState = currentState.becomeCandidate(newTerm, clusterSize)
        val requestVote = RequestVote(newTerm, log.latestAppended())
        AddressedRequest(cluster.peers.map(_ -> requestVote))
    }
  }

  def onBecomeFollower(newLeader: Option[NodeKey], newTerm: Term) = {
    if (currentState.isLeader) {
      // cancel HB for all nodes
      timers.sendHeartbeat.cancel(nodeKey)
      cluster.peers.foreach(timers.sendHeartbeat.cancel)
    }
    timers.receiveHeartbeat.reset(nodeKey)
    persistentState.currentTerm = newTerm
    currentState = currentState.becomeFollower(newLeader)
  }

  def onBecomeLeader(state: RaftNode[NodeKey]): AddressedRequest[NodeKey, A] = {
    val hb = makeDefaultHeartbeat()
    timers.receiveHeartbeat.cancel(currentState.id)
    val msgs = cluster.peers.map { nodeKey =>
      timers.sendHeartbeat.reset(nodeKey)
      (nodeKey, hb)
    }
    AddressedRequest(msgs)
  }

}
