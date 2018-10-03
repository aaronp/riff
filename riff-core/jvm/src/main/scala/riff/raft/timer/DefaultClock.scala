package riff.raft.timer

import java.util.concurrent.Executors.newScheduledThreadPool
import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}

import scala.concurrent.duration.FiniteDuration

class DefaultClock(
  sendHeartbeatTimeout: FiniteDuration,
  receiveHeartbeat: RandomTimer,
  schedulerService: ScheduledExecutorService = newScheduledThreadPool(1),
  cancelMayInterruptIfRunning: Boolean = true)
    extends RaftClock with AutoCloseable {
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
      }, receiveHeartbeat.next().toMillis, TimeUnit.MILLISECONDS)
      .asInstanceOf[ScheduledFuture[Unit]]

    next
  }

  override def close(): Unit = {
    schedulerService.shutdown()
  }
}

object DefaultClock {

  def apply(
    sendHeartbeatTimeout: FiniteDuration,
    receiveHeartbeat: RandomTimer,
    schedulerService: ScheduledExecutorService = java.util.concurrent.Executors.newScheduledThreadPool(1),
    cancelMayInterruptIfRunning: Boolean = true): RaftClock = {
    new DefaultClock(sendHeartbeatTimeout, receiveHeartbeat, schedulerService, cancelMayInterruptIfRunning)
  }
}
