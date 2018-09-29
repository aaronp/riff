package riff.raft.timer

class Timers(val clock: RaftClock) {

  class CancelableMap(doReset: TimerCallback[_] => clock.CancelT) {
    private var cancelable = Option.empty[clock.CancelT]

    def cancel() = {
      cancelable.foreach { c =>
        clock.cancelTimeout(c)
        cancelable = None
      }
    }

    def reset(callback: TimerCallback[_]): clock.CancelT = {
      cancel()
      val c = doReset(callback)
      cancelable = Option(c)
      c
    }
  }

  object receiveHeartbeat extends CancelableMap(clock.resetReceiveHeartbeatTimeout)

  object sendHeartbeat extends CancelableMap(clock.resetSendHeartbeatTimeout)
}
