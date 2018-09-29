package riff.raft.timer

import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}

import scala.concurrent.duration.FiniteDuration

class DefaultClock(
  sendHeartbeatTimeout: FiniteDuration,
  receiveHeartbeatTimeout: FiniteDuration,
  schedulerService: ScheduledExecutorService = java.util.concurrent.Executors.newScheduledThreadPool(1),
  cancelMayInterruptIfRunning: Boolean = true)
    extends RaftClock {
  override type CancelT = ScheduledFuture[Unit]

  override def cancelTimeout(c: ScheduledFuture[Unit]): Unit = c.cancel(cancelMayInterruptIfRunning)

  override def resetSendHeartbeatTimeout(callback: TimerCallback[_]): ScheduledFuture[Unit] = {
    val next = schedulerService
      .schedule(new Runnable() {
        override def run(): Unit = {
          callback.onSendHeartbeatTimeout()
          ()

        }
      }, sendHeartbeatTimeout.toMillis, TimeUnit.MILLISECONDS)
      .asInstanceOf[ScheduledFuture[Unit]]


    next
  }

  override def resetReceiveHeartbeatTimeout(callback: TimerCallback[_]): ScheduledFuture[Unit] = {
    val next = schedulerService
      .schedule(new Runnable() {
        override def run(): Unit = {
          callback.onReceiveHeartbeatTimeout()
          ()

        }
      }, receiveHeartbeatTimeout.toMillis, TimeUnit.MILLISECONDS)
      .asInstanceOf[ScheduledFuture[Unit]]

    next
  }
}

object DefaultClock {

  def apply(
    sendHeartbeatTimeout: FiniteDuration,
    receiveHeartbeatTimeout: FiniteDuration,
    schedulerService: ScheduledExecutorService = java.util.concurrent.Executors.newScheduledThreadPool(1),
    cancelMayInterruptIfRunning: Boolean = true): RaftClock = {
    new DefaultClock(
      sendHeartbeatTimeout,
      receiveHeartbeatTimeout,
      schedulerService,
      cancelMayInterruptIfRunning)
  }
}
