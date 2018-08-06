package riff.raft.node
import riff.raft.log.{LogCoords, LogEntry, RaftLog}
import riff.raft.messages.{RaftResponse, _}
import riff.raft.timer.{RaftTimer, Timers}
import riff.raft.{Term, node}

object NodeState {

  def inMemory[NodeKey, A](id: NodeKey)(implicit timer: RaftTimer[NodeKey]): NodeState[NodeKey, A] = {
    new NodeState[NodeKey, A](
      PersistentState.inMemory().cached(),
      RaftLog.inMemory[A](),
      new Timers[NodeKey](),
      RaftCluster[NodeKey](Nil),
      node.FollowerNode[NodeKey](id, None)
    )
  }

}

/**
  * The place where the different pieces of the system come together -- the glue code.
  *
  * I've looked at this a few different ways, but ultimately found this abstraction here to be the most readable,
  * and follows most closely what's laid out in the raft spec.
  *
  * It's not too generic/abstracted, but quite openly just orchestrates the pieces/interactions of the inputs
  * into a raft node.
  *
  * At the most general, it just
  *
  * @param persistentState
  * @param log
  * @param timers
  * @param cluster
  * @param initialState
  * @tparam NodeKey
  * @tparam A
  */
class NodeState[NodeKey, A](val persistentState: PersistentState[NodeKey],
                            val log: RaftLog[A],
                            val timers: Timers[NodeKey],
                            val cluster: RaftCluster[NodeKey],
                            initialState: RaftNode[NodeKey],
                            val maxAppendSize: Int = 1000)
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

  private[node] def raftNode(): RaftNode[NodeKey] = currentState

  protected def thisTerm() = persistentState.currentTerm

  /** Used by some client
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
        currentState = candidate.onRequestVoteResponse(from, cluster, voteResponse)

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

  def onSendHeartbeatTimeout(toNode: NodeKey): Result = {
    if (currentState.isLeader) {
      timers.sendHeartbeat.reset(currentState.id)
      AddressedRequest(toNode, makeHeartbeat())
    } else {
      NoOpOutput(s"Received send heartbeat timeout for $toNode, but we're ${currentState.role} in term ${thisTerm}")
    }
  }

  protected def makeHeartbeat() = AppendEntries(log.latestAppended(), thisTerm, log.latestCommit(), Array.empty[LogEntry[A]])

  def onReceiveHeartbeatTimeout(): Result = {
    onBecomeCandidate()
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

  def onBecomeCandidate(): AddressedRequest[NodeKey, A] = {
    val newTerm = thisTerm + 1
    persistentState.currentTerm = newTerm

    val clusterSize = cluster.size + 1
    currentState = currentState.onReceiveHeartbeatTimeout(newTerm, clusterSize)

    // write down that we're voting for ourselves
    persistentState.castVote(newTerm, nodeKey)

    val logState: LogCoords = log.latestAppended()
    val requestVote         = RequestVote(newTerm, logState)
    val requests            = cluster.peers.map(_ -> requestVote)
    AddressedRequest(requests)
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

  def onBecomeLeader(state: RaftNode[NodeKey]): Result = {
    val hb = makeHeartbeat()
    timers.receiveHeartbeat.cancel(currentState.id)
    val msgs = cluster.peers.map { nodeKey =>
      timers.sendHeartbeat.reset(nodeKey)
      (nodeKey, hb)
    }
    AddressedRequest(msgs)
  }

}
