package riff.raft.timer

import org.scalajs.dom.window

import riff.raft.NodeId
import scala.concurrent.duration.FiniteDuration

class DefaultTimer(
  callback: TimerCallback,
  sendHeartbeatTimeout: FiniteDuration,
  receiveHeartbeatTimeout: FiniteDuration)
    extends RaftTimer {

  type CancelT = Int

  def resetReceiveHeartbeatTimeout(raftNode: NodeId, previous: Option[CancelT]): CancelT = {
    previous.foreach(cancelTimeout)
    window.setTimeout(() => callback.onReceiveHeartbeatTimeout(raftNode), sendHeartbeatTimeout.toMillis)
  }

  def resetSendHeartbeatTimeout(raftNode: NodeId, previous: Option[CancelT]): CancelT = {
    previous.foreach(cancelTimeout)
    window.setTimeout(() => callback.onSendHeartbeatTimeout(raftNode), sendHeartbeatTimeout.toMillis)
  }

  def cancelTimeout(c: CancelT): Unit = {
    window.clearTimeout(c)
  }
}

object DefaultTimer {

  def apply(
    callback: TimerCallback,
    sendHeartbeatTimeout: FiniteDuration,
    receiveHeartbeatTimeout: FiniteDuration): RaftTimer = {
    new DefaultTimer(callback, sendHeartbeatTimeout, receiveHeartbeatTimeout)
  }
}
