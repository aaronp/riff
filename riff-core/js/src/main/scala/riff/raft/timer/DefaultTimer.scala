package riff.raft.timer

import org.scalajs.dom.window

import scala.concurrent.duration.FiniteDuration

class DefaultTimer(sendHeartbeatTimeout: FiniteDuration, receiveHeartbeatTimeout: FiniteDuration) extends RaftTimer {

  type CancelT = Int

  def resetReceiveHeartbeatTimeout(callback: TimerCallback): CancelT = {
    window.setTimeout(() => callback.onReceiveHeartbeatTimeout(), sendHeartbeatTimeout.toMillis)
  }

  def resetSendHeartbeatTimeout(callback: TimerCallback): CancelT = {
    window.setTimeout(() => callback.onSendHeartbeatTimeout(raftNode), sendHeartbeatTimeout.toMillis)
  }

  def cancelTimeout(c: CancelT): Unit = {
    window.clearTimeout(c)
  }
}

object DefaultTimer {

  def apply(
    sendHeartbeatTimeout: FiniteDuration,
    receiveHeartbeatTimeout: FiniteDuration): RaftTimer = {
    new DefaultTimer(sendHeartbeatTimeout, receiveHeartbeatTimeout)
  }
}
