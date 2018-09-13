package riff.raft.integration
package simulator

import riff.raft.log.LogAppendResult
import riff.raft.messages.{ReceiveHeartbeatTimeout, RequestOrResponse, SendHeartbeatTimeout, TimerMessage}
import riff.raft.node._
import riff.raft.timer.RaftTimer

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
                             defaultLatency: FiniteDuration,
                             newNode: (String, RaftCluster[String], RaftTimer[String]) => RaftNode[String, String])
    extends HasTimeline[TimelineType] {

  def killNode(nodeName: String) = {
    val found = clusterByName.get(nodeName)
    clusterByName = clusterByName - nodeName
    found
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

  private var clusterByName: Map[String, RaftNode[String, String]] = {
    clusterNodes
      .ensuring(_.distinct.size == clusterNodes.size)
      .map { name => name -> makeNode(name)
      }
      .toMap
  }

  // a handy place for breakpoints to watch for all updates
  private def updateTimeline(newTimeline: Timeline[TimelineType]) = {
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

  override def currentTimeline(): Timeline[TimelineType] = sharedSimulatedTimeline

  /**
    * convenience method to append to whatever the leader node is -- will error if there is no leader
    *
    * @param str
    * @return the append result
    */
  def appendToLeader(data: Array[String], latency: FiniteDuration = defaultLatency): Option[LogAppendResult] = {
    val ldr = currentLeader()
    ldr.appendIfLeader(data) match {
      case Some((appendResults, AddressedRequest(requests))) =>
        val newTimeline = requests.foldLeft(sharedSimulatedTimeline) {
          case (timeline, (to, msg)) =>
            val (newTimeline, _) = timeline.insertAfter(latency, SendRequest(ldr.raftNode().id, to, msg))
            newTimeline
        }
        updateTimeline(newTimeline)

        Some(appendResults)
      case None => None
    }
  }

  /** @param name the node whose RaftCluster this is
    * @return an implementation of RaftCluster for each node as a view onto the simulator
    */
  private def clusterForNode(name: String) = new RaftCluster[String] {
    override def peers: Iterable[String]        = clusterByName.keySet - name
    override def contains(key: String): Boolean = clusterByName.contains(key)
  }

  private def makeNode(name: String): RaftNode[String, String] = {
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

  def currentLeader(): RaftNode[String, String] = clusterByName(leader().get.id)

  def nodesWithRole(role: NodeRole) = clusterByName.values.filter(_.raftNode().role == role)

  /** @return the current leader (if there is one)
    */
  def leader(): Option[LeaderNodeState[String]] = {
    val leaders = clusterByName.values.map(_.raftNode()) collect {
      case leader: LeaderNodeState[String] => leader
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

  def advanceUntil(max: Int, latency: FiniteDuration = defaultLatency, predicate: AdvanceResult => Boolean): AdvanceResult = {
    val first: AdvanceResult = advance(latency)
    if (predicate(first)) {
      first
    } else {

      var mostRecentResult = first
      val (results, _) = (1 until max).view
        .map { _ =>
          try {
            val nextResult = advance(latency)
            mostRecentResult = nextResult
            nextResult
          } catch {
            case exp =>
              throw new Exception(s"${exp.getMessage}, previous result was: $mostRecentResult", exp)
          }
        }
        .span(r => !predicate(r))

      val list = results.toList

      require(list.size < max - 1, s"The condition never occurred in $max events")

      concatResults(first :: list)
    }
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
    list.last.copy(beforeTimeline = list.head.beforeTimeline, beforeStateByName = list.head.beforeStateByName, events = list.flatMap(_.events))
  }

  /**
    * pops the next event from the sharedSimulatedTimeline and enqueues the result onto the sharedSimulatedTimeline.
    *
    * @param latency
    * @return
    */
  private def advanceSafe(latency: FiniteDuration = 50.millis): Option[AdvanceResult] = {

    val beforeState: Map[String, NodeSnapshot[String]] = takeSnapshot()
    val beforeTimeline                                 = sharedSimulatedTimeline

    processNextEventInTheTimeline().map {
      case (e, node, result: NoOpResult) =>
        AdvanceResult(node, beforeState, beforeTimeline, e, result, sharedSimulatedTimeline, undeliveredTimeline, takeSnapshot())
      case (e, node, result @ AddressedRequest(msgs)) =>
        val newTimeline = msgs.foldLeft(sharedSimulatedTimeline) {
          case (time, (to, msg)) =>
            val (newTime, _) = time.insertAfter(latency, SendRequest(node, to, msg))
            newTime
        }
        updateTimeline(newTimeline)
        AdvanceResult(node, beforeState, beforeTimeline, e, result, newTimeline, undeliveredTimeline, takeSnapshot())
      case (e, node, result @ AddressedResponse(to, msg)) =>
        val (newTime, _) = sharedSimulatedTimeline.insertAfter(latency, SendResponse(node, to, msg))
        updateTimeline(newTime)
        AdvanceResult(node, beforeState, beforeTimeline, e, result, sharedSimulatedTimeline, undeliveredTimeline, takeSnapshot())
    }
  }

  def nodeFor(idx: Int): RaftNode[String, String] = {
    clusterByName.getOrElse(nameForIdx(idx), sys.error(s"Couldn't find ${nameForIdx(idx)} in ${clusterByName.keySet}"))
  }
  def snapshotFor(idx: Int): NodeSnapshot[String] = NodeSnapshot(nodeFor(idx))

  /**
    * move the time forward by one tick and route the event/msg to the respective node
    *
    * @return a tuple of the next event, recipient of the event, and result in an option, or None if no events are enqueued (which should never be the case, as we should always be sending heartbeats)
    */
  private def processNextEventInTheTimeline(): Option[(TimelineType, String, RaftNode[String, String]#Result)] = {

    def deliverMsg(from: String, to: String, timelineEvent: TimelineType, msg: RequestOrResponse[String, String]) = {
      clusterByName.get(to) match {
        case Some(node) =>
          node.onMessage(from, msg)
        case None =>
          markUndelivered(undeliveredTimeline.insertAfter(0.millis, timelineEvent)._1)

          NoOpResult(s"Can't deliver msg from $from to $to : $msg")
      }
    }
    def deliverTimerMsg(to: String, timelineEvent: TimelineType, msg: TimerMessage) = {
      clusterByName.get(to) match {
        case Some(node) => node.onTimerMessage(msg)
        case None =>
          markUndelivered(undeliveredTimeline.insertAfter(0.millis, timelineEvent)._1)
          NoOpResult(s"Can't deliver timer msg for $to : $msg")
      }
    }

    val popped = sharedSimulatedTimeline
      .pop()
      .map {
        case (newTimeline, e) =>
          updateTimeline(newTimeline)
          e
      }

    try {
      popped.map {
        case e @ SendTimeout(node)            => (e, node, deliverTimerMsg(node, e, SendHeartbeatTimeout))
        case e @ ReceiveTimeout(node: String) => (e, node, deliverTimerMsg(node, e, ReceiveHeartbeatTimeout))
        case e @ SendRequest(from, to, request) =>
          val result = deliverMsg(from, to, e, request)
          (e, to, result)
        case e @ SendResponse(from, to, response) =>
          val result = deliverMsg(from, to, e, response)
          (e, to, result)
        case other => sys.error(s"Unhandled simulated event $other")
      }
    } catch {
      case exp: Exception =>
        throw new Exception(s"Error processing $popped: ${exp.getMessage}", exp)
    }
  }

  /**
    * This class encloses over the 'sharedSimulatedTimeline', which is a var that can be altered
    *
    * @param forNode
    */
  private class SimulatedTimer(forNode: String) extends RaftTimer[String] {

    override type CancelT = (Long, TimelineType)

    private def scheduleTimeout(after: FiniteDuration, raftNode: TimelineType) = {
      val (newTimeline, entry) = sharedSimulatedTimeline.insertAfter(after, raftNode)
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
      updateTimeline(sharedSimulatedTimeline.remove(c))
    }
  }
}

object RaftSimulator {

  type NodeResult = RaftNodeResult[String, String]

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

  def newNode(name: String, cluster: RaftCluster[String], timer: RaftTimer[String]): RaftNode[String, String] = {
    val st8: RaftNode[String, String] = RaftNode.inMemory[String, String](name)(timer).withCluster(cluster)
    st8.timers.receiveHeartbeat.reset(name)
    st8
  }

  def clusterOfSize(n: Int): RaftSimulator = {
    new RaftSimulator(sendHeartbeatTimeouts, receiveHeartbeatTimeouts, (1 to n).map(nameForIdx).toList, 50.millis, newNode)
  }
}
