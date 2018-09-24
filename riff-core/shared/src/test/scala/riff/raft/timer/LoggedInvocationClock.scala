package riff.raft.timer

class LoggedInvocationClock extends RaftClock {
  override type CancelT = String
  private var receiveCalls = 0
  private var sendCalls = 0
  private var cancelCalls = 0

  def resetReceiveHeartbeatCalls(): Int = {
    val b4 = receiveCalls
    receiveCalls = 0
    b4
  }
  def resetSendHeartbeatCalls(): Int = {
    val b4 = sendCalls
    sendCalls = 0
    b4
  }
  def cancelHeartbeatCall(): Int = {
    val b4 = cancelCalls
    cancelCalls = 0
    b4
  }

  override def resetReceiveHeartbeatTimeout(callback: TimerCallback[_]): String = {
    receiveCalls += 1
    "" + receiveCalls
  }
  override def resetSendHeartbeatTimeout(callback: TimerCallback[_]): String = {
    sendCalls += 1
    "" + sendCalls

  }
  override def cancelTimeout(c: String): Unit = {
    cancelCalls += 1
  }
}
