package riff.monix
import monix.execution.{Cancelable, Scheduler}
import riff.raft.timer.{RaftTimer, TimerCallback}

import scala.concurrent.duration.FiniteDuration

class MonixTimer[A: TimerCallback](sendHeartbeatTimeout: FiniteDuration, receiveHeartbeatTimeout: FiniteDuration)(
    implicit sched: Scheduler)
    extends RaftTimer[A] {
  type CancelT = Cancelable

  override def cancelTimeout(c: Cancelable): Unit = c.cancel()

  override def resetSendHeartbeatTimeout(node: A, previous: Option[Cancelable]): Cancelable = {
    previous.foreach(cancelTimeout)
    val cancel: Cancelable = sched.scheduleOnce(sendHeartbeatTimeout) {
      TimerCallback[A].onSendHeartbeatTimeout(node)
      ()
    }
    cancel
  }

  override def resetReceiveHeartbeatTimeout(node: A, previous: Option[Cancelable]): Cancelable = {
    previous.foreach(cancelTimeout)
    val cancel: Cancelable = sched.scheduleOnce(receiveHeartbeatTimeout) {
      TimerCallback[A].onReceiveHeartbeatTimeout(node)
      ()
    }
    cancel
  }
}

object MonixTimer {

  def apply[A: TimerCallback](sendHeartbeatTimeout: FiniteDuration, receiveHeartbeatTimeout: FiniteDuration)(
      implicit sched: Scheduler = RiffSchedulers.DefaultScheduler): RaftTimer[A] = {
    new MonixTimer[A](sendHeartbeatTimeout, receiveHeartbeatTimeout)
  }
}
