package riff.raft.timer

/**
  * @tparam A the raft node type. A raft node and a timer go hand-in glove, but we'd have a chicken/egg problem if we
  *           built the timer into the node
  */
trait TimerCallback[Result] {
  def onSendHeartbeatTimeout(): Result
  def onReceiveHeartbeatTimeout(): Result
}

object TimerCallback {
  def onReceiveTimeout[Result](doOnReceive : => Result): onSendTimeout[Result] = {
    new onSendTimeout[Result](doOnReceive)
  }

  class onSendTimeout[Result] private[TimerCallback](doOnReceive : => Result) {
    def onSendTimeout(doOnSendTimeout : => Result) = {
      new TimerCallback[Result] {
        override def onSendHeartbeatTimeout(): Result = doOnSendTimeout
        override def onReceiveHeartbeatTimeout(): Result = doOnReceive
      }
    }
  }
}