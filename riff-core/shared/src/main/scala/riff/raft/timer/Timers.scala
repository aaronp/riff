package riff.raft.timer

class Timers(val timer: RaftTimer) {

  class CancelableMap(doReset: TimerCallback => timer.CancelT) {
    private var cancelable = Option.empty[timer.CancelT]

    def cancel() = {
      cancelable.foreach { c =>
        timer.cancelTimeout(c)
        cancelable = None
      }
    }

    def reset(callback: TimerCallback): timer.CancelT = {
      cancel()
      val c = doReset(callback)
      cancelable = Option(c)
      c
    }
  }

  object receiveHeartbeat extends CancelableMap(timer.resetReceiveHeartbeatTimeout)

  object sendHeartbeat extends CancelableMap(timer.resetSendHeartbeatTimeout)
}
