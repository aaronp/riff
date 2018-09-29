package riff.raft.timer

/**
  * @tparam A the raft node type. A raft node and a timer go hand-in glove, but we'd have a chicken/egg problem if we
  *           built the timer into the node
  */
trait TimerCallback {
  def onSendHeartbeatTimeout(): Unit
  def onReceiveHeartbeatTimeout(): Unit
}