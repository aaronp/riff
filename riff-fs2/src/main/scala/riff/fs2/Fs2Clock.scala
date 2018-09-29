package riff.fs2
import fs2.Scheduler
import riff.raft.NodeId
import riff.raft.timer.{RaftClock, TimerCallback}

import scala.concurrent.duration.FiniteDuration

class Fs2Clock(callback: TimerCallback, sendHeartbeatTimeout: FiniteDuration, receiveHeartbeatTimeout: FiniteDuration)(
  implicit sched: Scheduler)
    extends RaftClock {
  type CancelT = Int //Cancelable

  override def cancelTimeout(c: CancelT): Unit = {
    ///c.cancel(true)
    ???
  }

  override def resetSendHeartbeatTimeout(node: NodeId, previous: Option[CancelT]): CancelT = {
    previous.foreach(cancelTimeout)
//    sched.delayCancellable(sendHeartbeatTimeout)
//    cancel
    ???
  }

  override def resetReceiveHeartbeatTimeout(node: NodeId, previous: Option[CancelT]): CancelT = {
    previous.foreach(cancelTimeout)
//    val cancel: Cancelable = sched.scheduleOnce(receiveHeartbeatTimeout) {
//      TimerCallback[A].onReceiveHeartbeatTimeout(node)
//      ()
//    }
//    cancel
    ???
  }
}

object Fs2Clock {

  def apply(callback: TimerCallback, sendHeartbeatTimeout: FiniteDuration, receiveHeartbeatTimeout: FiniteDuration)(
    implicit sched: Scheduler) = {

    new Fs2Clock(callback, sendHeartbeatTimeout, receiveHeartbeatTimeout)
  }
}
