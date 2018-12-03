package riff.monix
import monix.execution.{Cancelable, Scheduler}
import monix.reactive.{Observable, Observer}
import monix.reactive.OverflowStrategy.DropOld
import monix.reactive.observers.Subscriber
import monix.reactive.subjects.Var
import riff.raft.NodeId
import riff.raft.messages.{ReceiveHeartbeatTimeout, SendHeartbeatTimeout, TimerMessage}
import riff.raft.timer.TimerCallback

/**
  * Provides a means to observe the nodes timeouts
  *
  * @param sched
  */
class ObservableTimerCallback(implicit sched: Scheduler) extends TimerCallback[Unit] {
  private val sendTimeoutsVar = Var[Boolean](false)

  def sendTimeout: Observable[TimerMessage] =
    sendTimeoutsVar.filter(identity).map(_ => SendHeartbeatTimeout).asyncBoundary(DropOld(2))
  override def onSendHeartbeatTimeout(): Unit = sendTimeoutsVar := true

  private val receiveTimeoutsVar = Var[Boolean](false)

  def receiveTimeouts: Observable[TimerMessage] =
    receiveTimeoutsVar.filter(identity).map(_ => ReceiveHeartbeatTimeout).asyncBoundary(DropOld(2))

  override def onReceiveHeartbeatTimeout(): Unit = receiveTimeoutsVar := true

  def subscribe(nodeId: NodeId, subscriber: Observer[TimerMessage]): Cancelable = {
    sendTimeout.dump(s"${nodeId} sendTimeout").subscribe(subscriber)
    receiveTimeouts.dump(s"${nodeId} receiveTimeouts").subscribe(subscriber)
  }
}
