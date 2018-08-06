package riff.raft.timer
import simulacrum.typeclass

/**
  * @tparam A the raft node type. A raft node and a timer go hand-in glove, but we'd have a chicken/egg problem if we
  *           built the timer into the node
  */
@typeclass trait TimerCallback[A] {
  def onSendHeartbeatTimeout(node: A): Unit
  def onReceiveHeartbeatTimeout(node: A): Unit
}
