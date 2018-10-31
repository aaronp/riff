package riff.monix
import monix.execution.{Cancelable, Scheduler}
import riff.raft.timer.{RaftClock, RandomTimer, TimerCallback}

import scala.concurrent.duration._

class MonixClock(sendHeartbeatTimeout: FiniteDuration, receiveHeartbeatMinMaxTimeout: (FiniteDuration, FiniteDuration))(implicit sched: Scheduler) extends RaftClock {
  type CancelT = Cancelable

  private val receivedRandom = new RandomTimer(receiveHeartbeatMinMaxTimeout._1, receiveHeartbeatMinMaxTimeout._2)

  override def cancelTimeout(c: Cancelable): Unit = c.cancel()

  override def resetSendHeartbeatTimeout(callback: TimerCallback[_]): Cancelable = {
    val cancel: Cancelable = sched.scheduleOnce(sendHeartbeatTimeout) {
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

  def apply()(implicit sched: Scheduler): MonixClock = apply(250.millis, (1.second, 2.seconds))

  def apply(sendHeartbeatTimeout: FiniteDuration, receiveHeartbeatMinMaxTimeout: (FiniteDuration, FiniteDuration))(implicit sched: Scheduler): MonixClock = {
    new MonixClock(sendHeartbeatTimeout, receiveHeartbeatMinMaxTimeout)
  }
}
