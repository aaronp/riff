package riff.raft.timer

import org.scalajs.dom.window

import scala.concurrent.duration.FiniteDuration

class DefaultClock(sendHeartbeatTimeout: FiniteDuration, receiveHeartbeatTimeout: FiniteDuration) extends RaftClock {

  type CancelT = Int

  def resetReceiveHeartbeatTimeout(callback: TimerCallback[_]): CancelT = {
    window.setTimeout(() => callback.onReceiveHeartbeatTimeout(), sendHeartbeatTimeout.toMillis)
  }

  def resetSendHeartbeatTimeout(callback: TimerCallback[_]): CancelT = {
    window.setTimeout(() => callback.onSendHeartbeatTimeout(), sendHeartbeatTimeout.toMillis)
  }

  def cancelTimeout(c: CancelT): Unit = {
    window.clearTimeout(c)
  }
}

object DefaultClock {

  def apply(sendHeartbeatTimeout: FiniteDuration, receiveHeartbeatTimeout: FiniteDuration) = {
    new DefaultClock(sendHeartbeatTimeout, receiveHeartbeatTimeout)
  }
}
