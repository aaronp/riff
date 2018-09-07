package riff.raft.timer

import org.scalajs.dom.window

import scala.concurrent.duration.FiniteDuration

class DefaultTimer[A: TimerCallback](sendHeartbeatTimeout: FiniteDuration, receiveHeartbeatTimeout: FiniteDuration) extends RaftTimer[A] {

  type CancelT = Int

  def resetReceiveHeartbeatTimeout(raftNode: A, previous: Option[CancelT]): CancelT = {
    previous.foreach(cancelTimeout)
    window.setTimeout(() => TimerCallback[A].onReceiveHeartbeatTimeout(raftNode), sendHeartbeatTimeout.toMillis)
  }

  def resetSendHeartbeatTimeout(raftNode: A, previous: Option[CancelT]): CancelT = {
    previous.foreach(cancelTimeout)
    window.setTimeout(() => TimerCallback[A].onSendHeartbeatTimeout(raftNode), sendHeartbeatTimeout.toMillis)
  }

  def cancelTimeout(c: CancelT): Unit = {
    window.clearTimeout(c)
  }
}

object DefaultTimer {
  def apply[A: TimerCallback](sendHeartbeatTimeout: FiniteDuration, receiveHeartbeatTimeout: FiniteDuration): RaftTimer[A] = {
    new DefaultTimer(sendHeartbeatTimeout, receiveHeartbeatTimeout)
  }
}
