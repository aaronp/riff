package riff.raft.integration.simulator
import riff.raft.node.RaftNode
import riff.raft.timer.{RaftClock, TimerCallback}

import scala.concurrent.duration.FiniteDuration

/**
  * This class encloses over the 'sharedSimulatedTimeline', which is a var that can be altered
  *
  * @param forNode
  */
private class SimulatedClock(simulator : RaftSimulator, forNode: String) extends RaftClock {
  import simulator._

  override type CancelT = (Long, TimelineType)

  private def scheduleTimeout(after: FiniteDuration, raftNode: TimelineType) = {
    val (newTimeline, entry) = currentTimeline.insertAfter(after, raftNode)
    updateTimeline(newTimeline)
    entry
  }
  override def cancelTimeout(c: (Long, TimelineType)): Unit = {
    val updated = currentTimeline.remove(c)
    updateTimeline(updated)
  }

  override def resetReceiveHeartbeatTimeout(callback: TimerCallback[_]): (Long, TimelineType) = {
    val timeout = nextReceiveTimeout.next()
    val raftNode = callback.asInstanceOf[RaftNode[_]].nodeId
    scheduleTimeout(timeout, ReceiveTimeout(raftNode))
  }

  override def resetSendHeartbeatTimeout(callback: TimerCallback[_]): (Long, TimelineType) = {
    val timeout = nextSendTimeout.next()
    val raftNode = callback.asInstanceOf[RaftNode[_]].nodeId
    scheduleTimeout(timeout, SendTimeout(raftNode))
  }
}