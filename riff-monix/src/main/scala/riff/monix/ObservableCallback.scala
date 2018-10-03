package riff.monix
import monix.execution.Scheduler
import monix.reactive.Observable
import monix.reactive.OverflowStrategy.DropOld
import monix.reactive.subjects.Var
import riff.raft.messages.{RaftMessage, ReceiveHeartbeatTimeout, SendHeartbeatTimeout}
import riff.raft.timer.TimerCallback

/**
  * Provides a means to observe the nodes timeouts
  *
  * @param sched
  */
class ObservableCallback(implicit sched: Scheduler) extends TimerCallback[Unit] {
  private val sendTimeoutsVar = Var[Boolean](false)

  def sendTimeout: Observable[RaftMessage[Nothing]] =
    sendTimeoutsVar.filter(identity).map(_ => SendHeartbeatTimeout).asyncBoundary(DropOld(2))
  override def onSendHeartbeatTimeout(): Unit = sendTimeoutsVar := true

  private val receiveTimeoutsVar = Var[Boolean](false)

  def receiveTimeouts: Observable[RaftMessage[Nothing]] =
    receiveTimeoutsVar.filter(identity).map(_ => ReceiveHeartbeatTimeout).asyncBoundary(DropOld(2))
  override def onReceiveHeartbeatTimeout(): Unit = receiveTimeoutsVar := true
}
