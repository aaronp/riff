package riff.monix
import monix.execution.Scheduler
import monix.reactive.Observable
import monix.reactive.subjects.Var
import riff.raft.messages.{ReceiveHeartbeatTimeout, SendHeartbeatTimeout}
import riff.raft.timer.TimerCallback

class ObservableCallback(implicit sched: Scheduler) extends TimerCallback[Unit] {
  private val sendTimeoutsVar = Var[Boolean](false)

  def sendTimeout: Observable[RaftStreamInput] =
    sendTimeoutsVar.filter(identity).map(_ => TimerInput(SendHeartbeatTimeout))
  override def onSendHeartbeatTimeout(): Unit = sendTimeoutsVar := true

  private val receiveTimeoutsVar = Var[Boolean](false)

  def receiveTimeouts: Observable[RaftStreamInput] =
    receiveTimeoutsVar.filter(identity).map(_ => TimerInput(ReceiveHeartbeatTimeout))
  override def onReceiveHeartbeatTimeout(): Unit = receiveTimeoutsVar := true
}
