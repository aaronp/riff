package riff.raft.integration
package simulator

import riff.raft.log.LogAppendResult
import riff.raft.messages.{ReceiveHeartbeatTimeout, RequestOrResponse, SendHeartbeatTimeout, TimerMessage}
import riff.raft.node.{RaftNode, _}
import riff.raft.timer.RaftTimer

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

/**
  * Exposes a means to have raft nodes generate messages and replies, as well as time-outs in a repeatable, testable,
  * fast way w/o having to test using 'eventually'.
  *
  * Instead of being a real, async, multi-threaded context, however, events are put on a shared [[Timeline]] which then
  * manually (via the [[riff.raft.integration.IntegrationTest]] advances time -- which just means popping the next event off the timeline,
  * feeding it to its targeted recipient, and pushing any resulting events from that application back onto the Timeline.
  *
  * As such, there should *always* be at least one event on the timeline after advancing time in a non-empty cluster, as
  * something should always have set a timeout (e.g. to receive or send a heartbeat).
  *
  * This implementation should resemble (a little bit) what other glue code looks like as a [[NodeState]] gets "lifted"
  * into some other context.
  *
  * That is to say, we may drive the implementation via akka actors, monix or fs2 streams, REST frameworks, etc.
  * Each of which would have a handle on single node and use its output to send/enqueue events, complete futures, whatever.
  *
  * Doing it this way we're just directly applying node request/responses to each other in a single (test) thread,
  * making life much simpler (and faster) to debug
  *
  */
class RaftSimulator private (nextSendTimeout: Iterator[FiniteDuration],
                             nextReceiveTimeout: Iterator[FiniteDuration],
                             clusterNodes: List[String],
                             val defaultLatency: FiniteDuration,
                             newNode: (String, RaftCluster, RaftTimer) => RaftNode[String])
    extends HasTimeline[TimelineType] {

  /** Simulates the effect of making a node un responsive by not sending requests/responses to the node.
    * The messages sent, however, will remain "popped" from the timeline, which is what we want... just as if e.g.
    * we sent a REST request which wasn't ever received.
    */
  def killNode(nodeName: String) = {
    stoppedNodes = stoppedNodes + nodeName
  }

  /**
    * Just makes the node respond to events again
    * @param nodeName the node to restart
    */
  def restartNode(nodeName: String) = {
    stoppedNodes = stoppedNodes - nodeName
  }

  // keeps track of events. This is a var that can change via 'updateTimeline', but doesn't need to be locked/volatile,
  // as our IntegrationTest is single-threaded (which makes stepping through, debugging and testing a *LOT* easier
  // as we don't have to wait for arbitrary times for things NOT to happen, or otherwise set short (but non-zero) delays
  // which are tuned for different environments. In the end, the tests prove that the system is functionally correct and
  // can drive itself via the events it generates, which gives us a HUGE amount of confidence that things are correct in
  // a repeatable way.
  //
  // In practice too, the NodeState which drives a given member in the cluster isn't threadsafe anyway, and so should be
  // put behind something else which drives that concern.
  private var sharedSimulatedTimeline = Timeline[TimelineType]()
  private var undeliveredTimeline     = Timeline[TimelineType]()

  // a separate collect of the nodes which we want to be unresponsive. We don't just remove them from the 'clusterByName'
  // map, as we still want the cluster view to be correct (e.g. the leader node should still know about the stopped/unresponsive members)
  private var stoppedNodes: Set[String] = Set.empty

  private var clusterByName: Map[String, RaftNode[String]] = {
    clusterNodes
      .ensuring(_.distinct.size == clusterNodes.size)
      .map { name => name -> makeNode(name)
      }
      .toMap
  }

  // a handy place for breakpoints to watch for all updates
  private def updateTimeline(newTimeline: Timeline[TimelineType]) = {
    require(newTimeline.currentTime >= sharedSimulatedTimeline.currentTime)
    sharedSimulatedTimeline = newTimeline
  }
  private def markUndelivered(newTimeline: Timeline[TimelineType]) = {
    undeliveredTimeline = newTimeline
  }

  def nextNodeName(): String      = Iterator.from(clusterByName.size).map(nameForIdx).dropWhile(clusterByName.keySet.contains).next()
  def addNodeCommand()            = RaftSimulator.addNode(nextNodeName())
  def removeNodeCommand(idx: Int) = RaftSimulator.removeNode(nameForIdx(idx))

  def updateCluster(data: String): Unit = {
    data match {
      case RaftSimulator.AddCommand(name) =>
        addNode(name)
      case RaftSimulator.RemoveCommand(name) =>
        removeNode(name)
      case _ =>
    }
  }

  override def currentTimeline(): Timeline[TimelineType] = {
    sharedSimulatedTimeline
  }

  /**
    * convenience method to append to whatever the leader node is -- will error if there is no leader
    *
    */
  def appendToLeader(data: Array[String], latency: FiniteDuration = defaultLatency): Option[LogAppendResult] = {
    val ldr   = currentLeader()
    val ldrId = ldr.state().id
    ldr.appendIfLeader(data).map {
      case (appendResults, AddressedRequest(requests)) =>
        val newTimeline = requests.zipWithIndex.foldLeft(currentTimeline) {
          case (timeline, ((to, msg), i)) =>
            //
            // ensure we insert the 'SendRequest' after any other SendRequest which came FROM this node
            // ...so we're not reordering our requests
            //
            val (newTimeline, _) = timeline.pushAfter(latency + i.millis, SendRequest(ldrId, to, msg)) {
              case SendRequest(from, _, _) => from == ldrId
            }

            newTimeline
        }
        updateTimeline(newTimeline)

        appendResults
    }
  }

  /** @param name the node whose RaftCluster this is
    * @return an implementation of RaftCluster for each node as a view onto the simulator
    */
  private def clusterForNode(name: String) = new RaftCluster {
    override def peers: Iterable[String]        = clusterByName.keySet - name
    override def contains(key: String): Boolean = clusterByName.contains(key)
  }

  private def makeNode(name: String): RaftNode[String] = {
    val node = newNode(name, clusterForNode(name), new SimulatedTimer(name))
    val newLog = node.log.onCommit { entry => updateCluster(entry.data)
    }
    node.withLog(newLog)
  }

  private def removeNode(name: String) = {
    clusterByName = clusterByName - name
  }

  private def addNode(name: String) = {
    if (!clusterByName.contains(name)) {
      val node = makeNode(name)
      clusterByName = clusterByName.updated(name, node)
    }
    this
  }

  def currentLeader(): RaftNode[String] = clusterByName(leaderState.id)

  def nodesWithRole(role: NodeRole): List[RaftNode[String]] = nodes.filter(_.state().role == role)

  def nodes(): List[RaftNode[String]] = clusterByName.values.toList

  def leaderState(): LeaderNodeState = leaderStateOpt.get

  /** @return the current leader (if there is one)
    */
  def leaderStateOpt(): Option[LeaderNodeState] = {
    val leaders = clusterByName.values.map(_.state()) collect {
      case leader: LeaderNodeState => leader
    }
    leaders.toList match {
      case Nil          => None
      case List(leader) => Option(leader)
      case many         => throw new IllegalStateException(s"Multiple simultaneous leaders! ${many}")
    }
  }

  def takeSnapshot(): Map[String, NodeSnapshot[String]] = clusterByName.map {
    case (key, n) => (key, NodeSnapshot(n))
  }

  /**
    * advance the timeline by one event
    *
    * @param latency
    * @return
    */
  def advance(latency: FiniteDuration = defaultLatency): AdvanceResult = {
    advanceSafe(latency).getOrElse(sys.error(s"Timeline is empty! This should never happen - there should always be timeouts queued"))
  }

  /**
    * flush the timeline applying all current events
    *
    * @param latency
    * @return
    */
  def advanceAll(latency: FiniteDuration = defaultLatency): AdvanceResult = advanceBy(currentTimeline().size)

  def advanceUntil(predicate: AdvanceResult => Boolean): AdvanceResult = {
    advanceUntil(100, defaultLatency, predicate)
  }

  def advanceUntilDebug(predicate: AdvanceResult => Boolean): AdvanceResult = {
    advanceUntil(100, defaultLatency, predicate, debug = true)
  }

  /**
    * Continues to advance the timeline until the given 'predicate' condition is true (up to 'max')
    *
    * @param max the maximum number to advance (the timeline should continually get timeouts registered, so without a max this could potentially loop forever)
    * @param latency the latency to use for enqueueing request/responses
    * @param predicate the condition we're moving towards
    * @param debug if true this spits out the timeline at each step as a convenience
    * @return the result
    */
  def advanceUntil(max: Int, latency: FiniteDuration, predicate: AdvanceResult => Boolean, debug: Boolean = false): AdvanceResult = {
    var next: AdvanceResult = advance(latency)

    def logDebug(result: AdvanceResult) = {
      if (debug) {
        println("- " * 50)
        println(debugString(Option(result.beforeTimeline)))
      }
    }

    logDebug(next)

    val list = ListBuffer[AdvanceResult](next)
    while (!predicate(next) && list.size < max) {
      next = advance(latency)

      logDebug(next)
      list += next
    }
    require(list.size < max, s"The condition was never met after $max iterations")
    concatResults(list.toList)
  }

  /**
    * flush 'nr' events
    *
    * @param nr the number of events to advance
    * @param latency
    * @return
    */
  def advanceBy(nr: Int, latency: FiniteDuration = defaultLatency): AdvanceResult = {
    val list = (1 to nr).map { _ => advance(latency)
    }.toList
    concatResults(list)
  }

  private def concatResults(list: List[AdvanceResult]): AdvanceResult = {
    val last: AdvanceResult = list.last
    last.copy(beforeTimeline = list.head.beforeTimeline, beforeStateByName = list.head.beforeStateByName, advanceEvents = list.flatMap(_.advanceEvents))
  }

  /**
    * pops the next event from the sharedSimulatedTimeline and enqueues the result onto the sharedSimulatedTimeline.
    *
    * @param latency the time buffer to assume for sending/receiving messages.
    * @return the result of advancing the next event in the timeline, if there was a next event
    */
  private def advanceSafe(latency: FiniteDuration = defaultLatency): Option[AdvanceResult] = {

    val beforeState: Map[String, NodeSnapshot[String]] = takeSnapshot()
    val beforeTimeline                                 = currentTimeline

    // pop one off our timeline stack, then subsequently update our sharedSimulatedTimeline
    beforeTimeline.pop().map {
      case (newTimeline, e) =>
        // first ensure the latest timeline is the advanced, popped one
        updateTimeline(newTimeline)
        val (recipient, result) = applyTimelineEvent(e, latency)
        val x: RaftNode[String]#Result = result
        AdvanceResult(recipient, beforeState, beforeTimeline, e, result, currentTimeline, undeliveredTimeline, takeSnapshot())
    }
  }

  def nodeFor(idx: Int): RaftNode[String] = {
    clusterByName.getOrElse(nameForIdx(idx), sys.error(s"Couldn't find ${nameForIdx(idx)} in ${clusterByName.keySet}"))
  }
  def snapshotFor(idx: Int): NodeSnapshot[String] = NodeSnapshot(nodeFor(idx))


  /**
    * Applies the next timeline event.
    *
    * Typically we allow the simulator to use 'advance*' methods to let the nodes drive their own logic, but this
    * CAN be called directlly by tests if we want to force an event (e.g. force an election, etc)
    *
    * @param nextEvent
    * @param currentTime
    * @param latency
    */
  def applyTimelineEvent(nextEvent: TimelineType, latency: FiniteDuration = defaultLatency, currentTime: Long = currentTimeline().currentTime): (String, RaftNode[String]#Result) = {
    val res @ (recipient, result) = processNextEvent(nextEvent, currentTime)
    applyResult(latency, recipient, result)
    res
  }

  private def applyResult(latency: FiniteDuration, node: String, result: RaftNode[String]#Result) = {
    result match {
      case _: NoOpResult =>
      case AddressedRequest(msgs) =>
        val newTimeline = msgs.zipWithIndex.foldLeft(currentTimeline) {
          case (time, ((to, msg), i)) =>
            val (newTime, _) =
              // if we just blindly use the latency, the we'll end up sending messages based off whatever the current
              // time is in the timeline, when really it should be done after the next
              time.pushAfter(latency + i.millis, SendRequest(node, to, msg)) {
                case SendRequest(from, _, _) => from == node
              }
            newTime
        }
        updateTimeline(newTimeline)
      case AddressedResponse(to, msg) =>
        val (newTime, _) = currentTimeline.insertAfter(latency, SendResponse(node, to, msg))
        updateTimeline(newTime)
    }
  }

  /**
    * process this event (assumed to be the next in the timeline)
    *
    * @return a tuple of the next event, recipient of the event, and result in an option, or None if no events are enqueued (which should never be the case, as we should always be sending heartbeats)
    */
  private def processNextEvent(nextEvent: TimelineType, currentTime: Long = currentTimeline().currentTime): (String, RaftNode[String]#Result) = {

    def deliverMsg(from: String, to: String, msg: RequestOrResponse[String]) = {
      clusterByName.get(to) match {
        case Some(node) if !stoppedNodes.contains(to) =>
          node.onMessage(from, msg)
        case _ =>
          markUndelivered(undeliveredTimeline.insertAfter(currentTime.millis, nextEvent)._1)
          NoOpResult(s"Can't deliver msg from $from to $to : $msg")
      }
    }
    def deliverTimerMsg(to: String, msg: TimerMessage) = {
      clusterByName.get(to) match {
        case Some(node) if !stoppedNodes.contains(to) => node.onTimerMessage(msg)
        case _ =>
          markUndelivered(undeliveredTimeline.insertAfter(currentTime.millis, nextEvent)._1)
          NoOpResult(s"Can't deliver timer msg for $to : $msg")
      }
    }

    nextEvent match {
      case SendTimeout(node)            => (node, deliverTimerMsg(node, SendHeartbeatTimeout))
      case ReceiveTimeout(node: String) => (node, deliverTimerMsg(node, ReceiveHeartbeatTimeout))
      case SendRequest(from, to, request) =>
        val result = deliverMsg(from, to, request)
        (to, result)
      case SendResponse(from, to, response) =>
        val result = deliverMsg(from, to, response)
        (to, result)
    }
  }

  /**
    * This class encloses over the 'sharedSimulatedTimeline', which is a var that can be altered
    *
    * @param forNode
    */
  private class SimulatedTimer(forNode: String) extends RaftTimer {

    override type CancelT = (Long, TimelineType)

    private def scheduleTimeout(after: FiniteDuration, raftNode: TimelineType) = {
      val (newTimeline, entry) = currentTimeline.insertAfter(after, raftNode)
      updateTimeline(newTimeline)
      entry
    }

    override def resetReceiveHeartbeatTimeout(raftNode: String, previous: Option[(Long, TimelineType)]): (Long, TimelineType) = {
      previous.foreach(cancelTimeout)
      val timeout = nextReceiveTimeout.next()
      require(raftNode == forNode, s"$forNode trying to reset a receive heartbeat for $raftNode")
      scheduleTimeout(timeout, ReceiveTimeout(raftNode))
    }
    override def resetSendHeartbeatTimeout(raftNode: String, previous: Option[(Long, TimelineType)]): (Long, TimelineType) = {
      previous.foreach(cancelTimeout)
      val timeout = nextSendTimeout.next()
      scheduleTimeout(timeout, SendTimeout(raftNode))
    }
    override def cancelTimeout(c: (Long, TimelineType)): Unit = {
      updateTimeline(currentTimeline.remove(c))
    }
  }

  override def toString(): String = debugString()

  def debugString(previousTimeline: Option[Timeline[TimelineType]] = None): String = {
    val timelineString = pretty("", previousTimeline)
    val strings = takeSnapshot().map {
      case (id, node) if stoppedNodes(id) => node.copy(name = s"${node.name} [stopped]").pretty()
      case (_, node) => node.pretty()
    }
    strings.mkString(s"${timelineString}\n", "\n", "")
  }
}

object RaftSimulator {

  type NodeResult = RaftNodeResult[String]

  /**
    * Our log has the simple 'string' type. Our RaftSimulator's state machine will check those entries against these commands
    * to add, remove (pause, etc) nodes
    */
  private val AddCommand       = "ADD:(.+)".r
  def addNode(name: String)    = s"ADD:${name}"
  private val RemoveCommand    = "REMOVE:(.+)".r
  def removeNode(name: String) = s"REMOVE:${name}"

  def sendHeartbeatTimeouts: Iterator[FiniteDuration] = Iterator(100.millis, 150.millis, 125.millis, 225.millis) ++ sendHeartbeatTimeouts
  def receiveHeartbeatTimeouts: Iterator[FiniteDuration] = {
    sendHeartbeatTimeouts.map(_ * 3)
  }

  def newNode(name: String, cluster: RaftCluster, timer: RaftTimer): RaftNode[String] = {
    val st8: RaftNode[String] = RaftNode.inMemory[String](name)(timer).withCluster(cluster)
    st8.timers.receiveHeartbeat.reset(name)
    st8
  }

  def clusterOfSize(n: Int): RaftSimulator = {
    new RaftSimulator(sendHeartbeatTimeouts, receiveHeartbeatTimeouts, (1 to n).map(nameForIdx).toList, 10.millis, newNode)
  }
}
