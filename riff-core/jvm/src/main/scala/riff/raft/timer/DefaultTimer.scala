package riff.raft.timer

import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}

import riff.raft.NodeId

import scala.concurrent.duration.FiniteDuration

class DefaultTimer(
  callback: TimerCallback,
  sendHeartbeatTimeout: FiniteDuration,
  receiveHeartbeatTimeout: FiniteDuration,
  schedulerService: ScheduledExecutorService = java.util.concurrent.Executors.newScheduledThreadPool(1),
  cancelMayInterruptIfRunning: Boolean = true)
    extends RaftTimer {
  override type CancelT = ScheduledFuture[Unit]

  override def cancelTimeout(c: ScheduledFuture[Unit]): Unit = c.cancel(cancelMayInterruptIfRunning)

  override def resetSendHeartbeatTimeout(
    raftNode: NodeId,
    previous: Option[ScheduledFuture[Unit]]): ScheduledFuture[Unit] = {
    val next = schedulerService
      .schedule(new Runnable() {
        override def run(): Unit = {
          callback.onSendHeartbeatTimeout(raftNode)
          ()

        }
      }, sendHeartbeatTimeout.toMillis, TimeUnit.MILLISECONDS)
      .asInstanceOf[ScheduledFuture[Unit]]

    previous.foreach(cancelTimeout)

    next
  }

  override def resetReceiveHeartbeatTimeout(
    raftNode: NodeId,
    previous: Option[ScheduledFuture[Unit]]): ScheduledFuture[Unit] = {
    previous.foreach(cancelTimeout)

    val next = schedulerService
      .schedule(new Runnable() {
        override def run(): Unit = {
          callback.onReceiveHeartbeatTimeout(raftNode)
          ()

        }
      }, receiveHeartbeatTimeout.toMillis, TimeUnit.MILLISECONDS)
      .asInstanceOf[ScheduledFuture[Unit]]

    next
  }
}

object DefaultTimer {

  def apply(
    callback: TimerCallback,
    sendHeartbeatTimeout: FiniteDuration,
    receiveHeartbeatTimeout: FiniteDuration,
    schedulerService: ScheduledExecutorService = java.util.concurrent.Executors.newScheduledThreadPool(1),
    cancelMayInterruptIfRunning: Boolean = true): RaftTimer = {
    new DefaultTimer(
      callback,
      sendHeartbeatTimeout,
      receiveHeartbeatTimeout,
      schedulerService,
      cancelMayInterruptIfRunning)
  }
}
