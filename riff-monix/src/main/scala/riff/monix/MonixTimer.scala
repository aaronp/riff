package riff.monix
import monix.execution.{Cancelable, Scheduler}
import riff.raft.timer.{RaftTimer, TimerCallback}

import scala.concurrent.duration._

class MonixTimer(sendHeartbeatTimeout: FiniteDuration, receiveHeartbeatTimeout: FiniteDuration)(
  implicit sched: Scheduler)
    extends RaftTimer {
  type CancelT = Cancelable

  override def cancelTimeout(c: Cancelable): Unit = c.cancel()

  override def resetSendHeartbeatTimeout(callback: TimerCallback): Cancelable = {
    val cancel: Cancelable = sched.scheduleOnce(sendHeartbeatTimeout) {
      callback.onSendHeartbeatTimeout()
      ()
    }
    cancel
  }

  override def resetReceiveHeartbeatTimeout(callback: TimerCallback): Cancelable = {
    val cancel: Cancelable = sched.scheduleOnce(receiveHeartbeatTimeout) {
      callback.onReceiveHeartbeatTimeout()
      ()
    }
    cancel
  }
}

object MonixTimer {

  def apply() = apply(300.millis, 100.millis)

  def apply(sendHeartbeatTimeout: FiniteDuration, receiveHeartbeatTimeout: FiniteDuration)(
    implicit sched: Scheduler = RiffSchedulers.computation.scheduler): RaftTimer = {
    new MonixTimer(sendHeartbeatTimeout, receiveHeartbeatTimeout)
  }
}
