package riff.raft.node
import riff.raft._
import riff.raft.log._
import riff.raft.messages.{RaftResponse, _}
import riff.raft.node.RoleCallback.{RoleChangeEvent, RoleEvent}
import riff.raft.timer.{RaftClock, TimerCallback, Timers}

import scala.reflect.ClassTag

object RaftNode {

  def inMemory[A](id: NodeId, maxAppendSize: Int = 10)(implicit clock: RaftClock): RaftNode[A] = {
    new RaftNode[A](
      PersistentState.inMemory().cached(),
      RaftLog.inMemory[A](),
      new Timers(clock),
      RaftCluster(Nil),
      node.FollowerNodeState(id, None),
      maxAppendSize
    )
  }

//  def appendResponseAsRaftNodeResult[A](response: Either[NotTheLeaderException, (LogAppendResult, AddressedRequest[A])]): RaftNodeResult[A] = {
//    response match {
//      case Left(exp) => NoOpResult(exp.getMessage)
//      case Right((_, requests)) => requests
//    }
//  }
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
  * @define WithSetup As this function returns a new node, care should be taken that it is used correctly in setting up the raft cluster;
  *                   'correctly' meaning that the returned instance is the one used to to communicate w/ the other nodes, as opposed
  *                   to an old reference.
  * @param persistentState the state used to track the currentTerm and vote state
  * @param log the RaftLog which stores the log entries
  * @param clock the timing apparatus for controlling timeouts
  * @param cluster this node's view of the cluster
  * @param initialState the initial role
  * @param maxAppendSize the maximum number of entries to send to a follower at a time when catching up the log
  * @param initialTimerCallback the callback which gets passed to the raft timer. If unspecified (e.g., left null), then 'this' RaftNode
  *                             will be used. In most usages, this **should** be set to an exterior callback which invokes this RaftNode
  *                             and does something with the result, since the result of a timeout is meaningful, as it will be a set of [[RequestVote]] messages, etc
  * @tparam A the log entry type
  */
class RaftNode[A](
  val persistentState: PersistentState,
  val log: RaftLog[A],
  val timers: Timers,
  val cluster: RaftCluster,
  initialState: NodeState,
  val maxAppendSize: Int,
  initialTimerCallback: TimerCallback[_] = null,
  val roleCallback: RoleCallback = RoleCallback.NoOp)
    extends RaftMessageHandler[A] with TimerCallback[RaftNodeResult[A]] with AutoCloseable { self =>

  private val timerCallback = Option(initialTimerCallback).getOrElse(this)

  private var currentState: NodeState = initialState

  private def updateState(newState: NodeState) = {
    val b4 = currentState.role
    currentState = newState
    if (b4 != currentState.role) {
      roleCallback.onEvent(RoleChangeEvent(currentTerm(), b4, currentState.role))
    }
  }

  override def nodeId: NodeId = currentState.id

  def state(): NodeState = currentState

  /**
    * Exposes this as a means for generating an AddressedRequest of messages together with the append result
    * from the leader's log
    *
    * @param data the data to append
    * @return the append result coupled w/ the append request to send if this node is the leader
    */
  def appendIfLeader(data: Array[A]): NodeAppendResult[A] = {
    currentState match {
      case leader: LeaderNodeState =>
        leader.makeAppendEntries[A](log, currentTerm(), data)
      case _ => NodeAppendResult(new NotTheLeaderException(nodeId, currentTerm, currentState.leader), AddressedRequest())
    }
  }

  override def onMessage(input: RaftMessage[A]): Result = {
    input match {
      case AddressedMessage(from, msg: RequestOrResponse[A]) => handleMessage(from, msg)
      case timer: TimerMessage => onTimerMessage(timer)
      case append : AppendData[A, _] => onAppendData(append)
    }
  }

  def onAppendData[P[_]](request : AppendData[A, P]): NodeAppendResult[A] = {
    appendIfLeader(request.values)
  }

  final def createAppendFor(data: A, theRest: A*)(implicit tag: ClassTag[A]): Result =
    appendIfLeader(data +: theRest.toArray)

  /**
    * Applies requests and responses coming to the node state and replies w/ any resulting messages
    *
    * @param from the node from which this message is received
    * @param msg the Raft message
    * @return and resulting messages (requests or responses)
    */
  def handleMessage(from: NodeId, msg: RequestOrResponse[A]): Result = {
    msg match {
      case request: RaftRequest[A] => AddressedResponse(from, onRequest(from, request))
      case reply: RaftResponse => onResponse(from, reply)
    }
  }

  /**
    * Handle a response coming from 'from'
    *
    * @param from the originating node
    * @param reply the response
    * @return any messages resulting from having processed this response
    */
  def onResponse(from: NodeId, reply: RaftResponse): Result = {
    reply match {
      case voteResponse: RequestVoteResponse => onRequestVoteResponse(from, voteResponse)
      case appendResponse: AppendEntriesResponse => onAppendEntriesResponse(from, appendResponse)
    }
  }

  def onRequestVoteResponse(from: NodeId, voteResponse: RequestVoteResponse): Result = {
    currentState match {
      case candidate: CandidateNodeState =>
        val newState = candidate.onRequestVoteResponse(from, cluster, voteResponse)

        updateState(newState)

        if (currentState.isLeader) {
          // notify leader change
          onBecomeLeader()
        } else {
          NoOpResult(s"Got vote ${voteResponse}, vote state is now : ${candidate.candidateState()}")
        }
      case _ =>
        NoOpResult(s"Got vote ${voteResponse} while in role ${currentState.role}, term ${currentTerm}")
    }
  }

  /**
    * We're either the leader and should update our peer view/commit log, or aren't and should ignore it
    *
    * @param appendResponse
    * @return the committed log coords resulting from having applied this response and the state output (either a no-op or a subsequent [[AppendEntries]] request)
    */
  def onAppendEntriesResponse(from: NodeId, appendResponse: AppendEntriesResponse): LeaderCommittedResult[A] = {
    currentState match {
      case leader: LeaderNodeState =>
        leader.onAppendResponse(from, log, currentTerm, appendResponse, maxAppendSize)

      case _ =>
        val result = NoOpResult(
          s"Ignoring append response from $from as we're in role ${currentState.role}, term ${currentTerm}")
        LeaderCommittedResult(Nil, result)

    }
  }

  def onTimerMessage(timeout: TimerMessage): Result = {
    timeout match {
      case ReceiveHeartbeatTimeout => onReceiveHeartbeatTimeout()
      case SendHeartbeatTimeout => onSendHeartbeatTimeout()
    }
  }

  private def createAppendOnHeartbeatTimeout(leader: LeaderNodeState, toNode: NodeId) = {
    leader.clusterView.stateForPeer(toNode) match {
      // we don't know what state this node is in - send our default heartbeat
      case None => makeDefaultHeartbeat()

      // we're at the beginning of the log - send some data
      case Some(Peer(1, 0)) =>
        AppendEntries(LogCoords.Empty, currentTerm, log.latestCommit(), log.entriesFrom(1, maxAppendSize))

      // the state where we've not yet matched a peer, so we're trying to send ever-decreasing indices,
      // using the 'nextIndex' (which should be decrementing on each fail)
      case Some(Peer(nextIndex, 0)) =>
        log.coordsForIndex(nextIndex) match {
          // "This should never happen" (c)
          // potential error situation where we have a 'nextIndex' -- resend the default heartbeat
          case None => makeDefaultHeartbeat()
          case Some(previous) => AppendEntries(previous, currentTerm, log.latestCommit(), Array.empty[LogEntry[A]])
        }

      // the normal success state where we send our expected previous coords based on the match index
      case Some(Peer(nextIndex, matchIndex)) =>
        log.coordsForIndex(matchIndex) match {
          case None =>
            // "This should never happen" (c)
            // potential error situation where we have a 'nextIndex' -- resend the default heartbeat
            makeDefaultHeartbeat()
          case Some(previous) =>
            AppendEntries(previous, currentTerm, log.latestCommit(), log.entriesFrom(nextIndex, maxAppendSize))
        }
    }
  }

  def onSendHeartbeatTimeout(): Result = {
    currentState match {
      case leader: LeaderNodeState =>
        resetSendHeartbeat()

        val msgs = cluster.peers.map { toNode =>
          val heartbeat = createAppendOnHeartbeatTimeout(leader, toNode)
          toNode -> heartbeat
        }

        AddressedRequest(msgs)
      case _ =>
        NoOpResult(s"Received send heartbeat timeout, but we're ${currentState.role} in term ${currentTerm}")
    }
  }

  private def makeDefaultHeartbeat() =
    AppendEntries(log.latestAppended(), currentTerm, log.latestCommit(), Array.empty[LogEntry[A]])

  def onReceiveHeartbeatTimeout(): AddressedRequest[A] = onBecomeCandidateOrLeader()

  def onRequest(from: NodeId, request: RaftRequest[A]): RaftResponse = {
    request match {
      case append: AppendEntries[A] => onAppendEntries(from, append)
      case vote: RequestVote => onRequestVote(from, vote)
    }
  }

  def onAppendEntries(from: NodeId, append: AppendEntries[A]): AppendEntriesResponse = {
    val beforeTerm = currentTerm
    val doAppend = if (beforeTerm < append.term) {
      onBecomeFollower(Option(from), append.term)
      resetReceiveHeartbeat()
      false
    } else if (beforeTerm > append.term) {
      // ignore/fail if we get an append for an earlier term
      false
    } else {
      // we're supposedly the leader of this term ... ???
      currentState match {
        case _: LeaderNodeState => false
        case follower @ FollowerNodeState(_, None) =>
          updateState(follower.copy(leader = Option(from)))
          roleCallback.onNewLeader(currentTerm, from)
          resetReceiveHeartbeat()
          true
        case _ =>
          resetReceiveHeartbeat()
          true
      }
    }

    if (doAppend) {
      val result: AppendEntriesResponse = log.onAppend(currentTerm, append)
      if (result.success) {
        log.commit(append.commitIndex)
      }
      result
    } else {
      AppendEntriesResponse.fail(currentTerm())
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
  def onRequestVote(from: NodeId, forRequest: RequestVote): RequestVoteResponse = {
    val beforeTerm = currentTerm
    val reply = persistentState.castVote(log.latestAppended(), from, forRequest)

    // regardless of granting the vote or not, if we just saw a later term, we need to be a follower
    // ... and if we are (soon to be were) leader, we have to transition (cancel heartbeats, etc)
    if (beforeTerm < reply.term) {
      onBecomeFollower(None, reply.term)
    }
    reply
  }

  def onBecomeCandidateOrLeader(): AddressedRequest[A] = {
    val newTerm = currentTerm + 1
    persistentState.currentTerm = newTerm

    // write down that we're voting for ourselves
    persistentState.castVote(newTerm, nodeId)

    // this election may end up being a split-brain, or we may have been disconnected. At any rate,
    // we need to reset our heartbeat timeout
    resetReceiveHeartbeat()

    cluster.numberOfPeers match {
      case 0 =>
        updateState(currentState.becomeLeader(cluster))
        onBecomeLeader()
      case clusterSize =>
        updateState(currentState.becomeCandidate(newTerm, clusterSize + 1))
        val requestVote = RequestVote(newTerm, log.latestAppended())
        AddressedRequest(cluster.peers.map(_ -> requestVote))
    }
  }

  def onBecomeFollower(newLeader: Option[NodeId], newTerm: Term) = {
    if (currentState.isLeader) {
      // cancel HB for all nodes
      cancelSendHeartbeat()
    }
    persistentState.currentTerm = newTerm
    newLeader.foreach(roleCallback.onNewLeader(currentTerm(), _))
    updateState(currentState.becomeFollower(newLeader))
  }

  def onBecomeLeader(): AddressedRequest[A] = {
    val hb = makeDefaultHeartbeat()
    cancelReceiveHeartbeat()
    resetSendHeartbeat()
    roleCallback.onNewLeader(currentTerm, nodeId)
    AddressedRequest(cluster.peers.map(_ -> hb))
  }

  def cancelReceiveHeartbeat(): Unit = {
    timers.receiveHeartbeat.cancel()
  }

  def cancelSendHeartbeat(): Unit = {
    timers.sendHeartbeat.cancel()
  }

  private[this] def resetSendHeartbeat(): timers.clock.CancelT = {
    timers.sendHeartbeat.reset(timerCallback)
  }

  /** This is NOT intended to be called directly, but is managed internally
    *
    * @return the cancellable timer result.
    */
  def resetReceiveHeartbeat(): timers.clock.CancelT = {
    timers.receiveHeartbeat.reset(timerCallback)
  }

  def currentTerm(): Term = persistentState.currentTerm

  /** $WithSetup
    *
    * a convenience builder method to create a new raft node w/ the given raft log
    *
    * @return a new node state
    */
  def withLog(newLog: RaftLog[A]): RaftNode[A] = {
    new RaftNode(persistentState, newLog, timers, cluster, state, maxAppendSize, timerCallback, roleCallback)
  }

  /** $WithSetup
    *
    * a convenience builder method to create a new raft node w/ the given cluster
    *
    * @return a new node state
    */
  def withCluster(newCluster: RaftCluster): RaftNode[A] = {
    new RaftNode(persistentState, log, timers, newCluster, state, maxAppendSize, timerCallback, roleCallback)
  }

  /** $WithSetup
    *
    * @param newTimerCallback the timer callback
    * @return a copy of thew RaftNode which uses the given timer callback
    */
  def withTimerCallback(newTimerCallback: TimerCallback[_]): RaftNode[A] = {
    new RaftNode(persistentState, log, timers, cluster, state, maxAppendSize, newTimerCallback, roleCallback)
  }

  /** $WithSetup
    *
    * A convenience method for exposing 'withRoleCallback' with a function instead of a [[RoleCallback]], so users
    * can invoke via:
    * {{{
    *   val initial : RaftNode[T] = ...
    *   initial.withRoleCallback { event =>
    *     // do something w/ the event here
    *     println(event)
    *   }
    * }}}
    *
    * @param f the callback function
    * @return a new RaftNode w/ the given callback added
    */
  def withRoleCallback(f: RoleEvent => Unit): RaftNode[A] = withRoleCallback(RoleCallback(f))

  /** $WithSetup
    *
    * Adds the given callback to this node to be invoked whenever a [[RoleEvent]] takes place
    *
    * @param f the callback function
    * @return a new RaftNode w/ the given callback added
    */
  def withRoleCallback(newRoleCallback: RoleCallback): RaftNode[A] = {
    new RaftNode(persistentState, log, timers, cluster, state, maxAppendSize, timerCallback, newRoleCallback)
  }

  override def toString() = {
    s"""RaftNode ${currentState.id} {
       |  timers : $timers
       |  cluster : $cluster
       |  currentState : $currentState
       |  log : ${log}
       |}""".stripMargin
  }

  override def close(): Unit = {
    cancelSendHeartbeat()
    cancelReceiveHeartbeat()
    timerCallback match {
      case closable: AutoCloseable if ! (closable eq this) => closable.close()
      case _ =>
    }
  }
}
