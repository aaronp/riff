package riff.monix
import monix.execution.{Cancelable, Scheduler}
import riff.raft.timer.{RaftClock, RandomTimer, TimerCallback}

import scala.concurrent.duration._

class MonixClock(
  sendHeartbeatTimeout: FiniteDuration,
  receiveHeartbeatTimeout: FiniteDuration,
  randPercentageOfTimeout: Double)(implicit sched: Scheduler)
    extends RaftClock {
  type CancelT = Cancelable

  private val sendRandom = new RandomTimer(sendHeartbeatTimeout, randPercentageOfTimeout)
  private val receivedRandom = new RandomTimer(receiveHeartbeatTimeout, randPercentageOfTimeout)

  override def cancelTimeout(c: Cancelable): Unit = c.cancel()

  override def resetSendHeartbeatTimeout(callback: TimerCallback[_]): Cancelable = {
    val cancel: Cancelable = sched.scheduleOnce(sendRandom.next()) {
      callback.onSendHeartbeatTimeout()
      ()
    }
    cancel
  }

  override def resetReceiveHeartbeatTimeout(callback: TimerCallback[_]): Cancelable = {
    val cancel: Cancelable = sched.scheduleOnce(receivedRandom.next()) {
      callback.onReceiveHeartbeatTimeout()
      ()
    }
    cancel
  }
}

object MonixClock {

  def apply()(implicit sched: Scheduler): MonixClock = apply(250.millis, 1.second)

  def apply(
    sendHeartbeatTimeout: FiniteDuration,
    receiveHeartbeatTimeout: FiniteDuration,
    randPercentageOfTimeout: Double = 0.2)(implicit sched: Scheduler): MonixClock = {
    new MonixClock(sendHeartbeatTimeout, receiveHeartbeatTimeout, randPercentageOfTimeout)
  }
}
