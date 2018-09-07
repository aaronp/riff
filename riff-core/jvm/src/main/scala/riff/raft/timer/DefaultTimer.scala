package riff.raft.timer

import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}

import scala.concurrent.duration.FiniteDuration

class DefaultTimer[A: TimerCallback](sendHeartbeatTimeout: FiniteDuration,
                                     receiveHeartbeatTimeout: FiniteDuration,
                                     schedulerService: ScheduledExecutorService = java.util.concurrent.Executors.newScheduledThreadPool(1),
                                     cancelMayInterruptIfRunning: Boolean = true)
    extends RaftTimer[A] {
  override type CancelT = ScheduledFuture[Unit]

  override def cancelTimeout(c: ScheduledFuture[Unit]): Unit = c.cancel(cancelMayInterruptIfRunning)

  override def resetSendHeartbeatTimeout(raftNode: A, previous: Option[ScheduledFuture[Unit]]): ScheduledFuture[Unit] = {
    val next = schedulerService
      .schedule(new Runnable() {
        override def run(): Unit = {
          TimerCallback[A].onSendHeartbeatTimeout(raftNode)
          ()

        }
      }, sendHeartbeatTimeout.toMillis, TimeUnit.MILLISECONDS)
      .asInstanceOf[ScheduledFuture[Unit]]

    previous.foreach(cancelTimeout)

    next
  }

  override def resetReceiveHeartbeatTimeout(raftNode: A, previous: Option[ScheduledFuture[Unit]]): ScheduledFuture[Unit] = {
    previous.foreach(cancelTimeout)

    val next = schedulerService
      .schedule(new Runnable() {
        override def run(): Unit = {
          TimerCallback[A].onReceiveHeartbeatTimeout(raftNode)
          ()

        }
      }, receiveHeartbeatTimeout.toMillis, TimeUnit.MILLISECONDS)
      .asInstanceOf[ScheduledFuture[Unit]]

    next
  }
}

object DefaultTimer {
  def apply[A: TimerCallback](sendHeartbeatTimeout: FiniteDuration,
                              receiveHeartbeatTimeout: FiniteDuration,
                              schedulerService: ScheduledExecutorService = java.util.concurrent.Executors.newScheduledThreadPool(1),
                              cancelMayInterruptIfRunning: Boolean = true): RaftTimer[A] = {
    new DefaultTimer(sendHeartbeatTimeout, receiveHeartbeatTimeout, schedulerService, cancelMayInterruptIfRunning)
  }
}
