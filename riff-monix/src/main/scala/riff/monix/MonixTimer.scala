package riff.monix
import monix.execution.{Cancelable, Scheduler}
import riff.raft.NodeId
import riff.raft.timer.{RaftTimer, TimerCallback}

import scala.concurrent.duration.FiniteDuration

class MonixTimer(callback: TimerCallback, sendHeartbeatTimeout: FiniteDuration, receiveHeartbeatTimeout: FiniteDuration)(
  implicit sched: Scheduler)
    extends RaftTimer {
  type CancelT = Cancelable

  override def cancelTimeout(c: Cancelable): Unit = c.cancel()

  override def resetSendHeartbeatTimeout(node: NodeId, previous: Option[Cancelable]): Cancelable = {
    previous.foreach(cancelTimeout)
    val cancel: Cancelable = sched.scheduleOnce(sendHeartbeatTimeout) {
      callback.onSendHeartbeatTimeout(node)
      ()
    }
    cancel
  }

  override def resetReceiveHeartbeatTimeout(node: NodeId, previous: Option[Cancelable]): Cancelable = {
    previous.foreach(cancelTimeout)
    val cancel: Cancelable = sched.scheduleOnce(receiveHeartbeatTimeout) {
      callback.onReceiveHeartbeatTimeout(node)
      ()
    }
    cancel
  }
}

object MonixTimer {

  def apply(callback: TimerCallback, sendHeartbeatTimeout: FiniteDuration, receiveHeartbeatTimeout: FiniteDuration)(
    implicit sched: Scheduler = RiffSchedulers.DefaultScheduler): RaftTimer = {
    new MonixTimer(callback, sendHeartbeatTimeout, receiveHeartbeatTimeout)
  }
}
