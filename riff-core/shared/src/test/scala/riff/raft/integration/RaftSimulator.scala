package riff.raft.integration

import riff.raft.messages.{RaftRequest, RaftResponse, ReceiveHeartbeatTimeout, SendHeartbeatTimeout}
import riff.raft.node._
import riff.raft.timer.RaftTimer

import scala.concurrent.duration._

/**
  * Exposes a means to have raft nodes generate messages and replies, as well as time-outs in a repeatable, testable,
  * fast way w/o having to test using 'eventually'.
  *
  * This implementation should resemble (a little bit) what other glue code looks like as a [[RaftNode]] gets "lifted"
  * into some other context.
  *
  * That is to say, we may drive the implementation via akka actors, monix or fs2 streams, REST frameworks, etc.
  * Each of which would have a handle on single node and use its output to send/enqueue events, complete futures, whatever.
  *
  * Doing it this way we're just directly applying node request/responses to each other in a single (test) thread,
  * making life much simpler (and faster) to debug
  *
  */
class RaftSimulator private (val nextTimeout: Iterator[FiniteDuration],
                             clusterNodes: List[String],
                             newNode: (String, RaftTimer[String]) => NodeState[String, String]) {

  import RaftSimulator._
  // keeps track of events
  private var timeline = Timeline()

  private val clusterByName: Map[String, NodeState[String, String]] = {
    clusterNodes
      .ensuring(_.distinct.size == clusterNodes.size)
      .map { name => name -> newNode(name, new SimulatedTimer(name))
      }
      .toMap
  }

  /** @return the current leader (if there is one)
    */
  def leader(): Option[LeaderNode[String]] = {
    val leaders = clusterByName.values.map(_.raftNode()) collect {
      case leader: LeaderNode[String] => leader
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
    * pops the next event from the timeline and enqueues the result onto the timeline.
    *
    * @param latency
    * @return
    */
  def advance(latency: FiniteDuration = 50.millis): Option[AdvanceResult] = {

    val beforeState: Map[String, NodeSnapshot[String]] = takeSnapshot()
    val beforeTimeline = timeline

    processNextEventInTheTimeline().map {
      case (e, node, result: NoOpOutput) =>
        AdvanceResult(node, beforeState, beforeTimeline, e, result, timeline, takeSnapshot())
      case (e, node, result @ AddressedRequest(msgs)) =>
        val newTimeline = msgs.foldLeft(timeline) {
          case (time, (to, msg)) =>
            val (newTime, _) = time.insertAfter(latency, SendRequest(node, to, msg))
            newTime
        }
        timeline = newTimeline
        AdvanceResult(node, beforeState, beforeTimeline, e, result, newTimeline, takeSnapshot())
      case (e, node, result @ AddressedResponse(to, msg)) =>
        val (newTime, _) = timeline.insertAfter(latency, SendResponse(node, to, msg))
        timeline = newTime
        AdvanceResult(node, beforeState, beforeTimeline, e, result, timeline, takeSnapshot())
    }
  }

  /**
    * move the time forward by one tick and route the event/msg to the respective node
    *
    * @return a tuple of the next event, recipient of the event, and result in an option, or None if no events are enqueued (which should never be the case, as we should always be sending heartbeats)
    */
  private def processNextEventInTheTimeline(): Option[(TimelineType, String, NodeState[String, String]#Result)] = {
    timeline
      .pop()
      .map {
        case (newTimeline, e) =>
          timeline = newTimeline
          e
      }
      .map {
        case e @ SendTimeout(from, to)            => (e, from, clusterByName(from).onTimerMessage(SendHeartbeatTimeout(to)))
        case e @ ReceiveTimeout(node: String)     => (e, node, clusterByName(node).onTimerMessage(ReceiveHeartbeatTimeout))
        case e @ SendRequest(from, to, request)   => (e, to, clusterByName(to).onMessage(from, request))
        case e @ SendResponse(from, to, response) => (e, to, clusterByName(to).onMessage(from, response))
        case other                                => sys.error(s"Unhandled simulated event $other")
      }
  }

  private class SimulatedTimer(forNode: String) extends RaftTimer[String] {

    override type CancelT = (Long, Any)

    private def scheduleTimeout(after: FiniteDuration, raftNode: TimelineType) = {
      val (newTimeline, entry) = timeline.insertAfter(after, raftNode)
      timeline = newTimeline
      entry
    }

    override def resetReceiveHeartbeatTimeout(raftNode: String, previous: Option[(Long, Any)]): (Long, Any) = {
      previous.foreach(cancelTimeout)
      val timeout = nextTimeout.next()
      require(raftNode == forNode, s"$forNode trying to reset a receive heartbeat for $raftNode")
      scheduleTimeout(timeout, ReceiveTimeout(raftNode))
    }
    override def resetSendHeartbeatTimeout(raftNode: String, previous: Option[(Long, Any)]): (Long, Any) = {
      previous.foreach(cancelTimeout)
      val timeout = nextTimeout.next()
      scheduleTimeout(timeout, SendTimeout(forNode, raftNode))
    }
    override def cancelTimeout(c: (Long, Any)): Unit = {
      val removed = timeline.remove(c)
      timeline = removed
    }
  }
}

object RaftSimulator {

  type NodeResult = NodeStateOutput[String, String]

  sealed trait TimelineType
  case class SendTimeout(from: String, to: String)                               extends TimelineType
  case class ReceiveTimeout(node: String)                                        extends TimelineType
  case class SendRequest(from: String, to: String, request: RaftRequest[String]) extends TimelineType
  case class SendResponse(from: String, to: String, request: RaftResponse)       extends TimelineType

  case class AdvanceResult(node: String,
                           beforeStateByName: Map[String, NodeSnapshot[String]],
                           beforeTimeline: Timeline,
                           event: TimelineType,
                           result: NodeResult,
                           afterTimeline: Timeline,
                           afterStateByName: Map[String, NodeSnapshot[String]]) {

    def beforeState(idx: Int) = beforeStateByName(nameForIdx(idx))
    def afterState(idx: Int)  = afterStateByName(nameForIdx(idx))

    def stateChanges: Map[String, NodeSnapshot[String]] = afterStateByName.filterNot {
      case (key, st8) => beforeStateByName(key) == st8
    }

    override def toString = {

      val newEvents = afterTimeline.diff(beforeTimeline)
      s"""$node processed $event:
         |  $result
         |
         |Before State:
         |${beforeStateByName.mapValues(_.pretty()).mkString("\n")}
         |${stateChanges.size} Changes:
         |${stateChanges.mapValues(_.pretty()).mkString("\n")}
         |
         |${newEvents.size} new enqueued events: ${newEvents.mkString("\n")}""".stripMargin
    }
  }

  def timeouts: Iterator[FiniteDuration] = Iterator(100.millis, 150.millis, 125.millis, 225.millis) ++ timeouts

  def clusterOfSize(n: Int): RaftSimulator = {
    def newNode(name: String, timer: RaftTimer[String]): NodeState[String, String] = {
      val st8: NodeState[String, String] = NodeState.inMemory[String, String](name)(timer)
      st8.timers.receiveHeartbeat.reset(name)
      st8
    }

    new RaftSimulator(timeouts, (1 to n).map(nameForIdx).toList, newNode)
  }
}
