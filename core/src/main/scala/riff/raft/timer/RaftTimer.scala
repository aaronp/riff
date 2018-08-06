package riff.raft.timer

import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}

import scala.concurrent.duration.FiniteDuration

/**
  * Represents the functions required to control a member node's election and heartbeat timeouts
  *
  */
trait RaftTimer[A] {
  type CancelT

  /**
    * Resets the heartbeat timeout for the given node.
    *
    * It contract is assumed that this function will be called periodically from the node passed in,
    * and it is up to the implementation to invoking 'oElectionTimeout' should it not be invoked
    * within a certain time
    *
    * @param node
    */
  def resetReceiveHeartbeatTimeout(raftNode: A, previous: Option[CancelT]): CancelT

  def resetSendHeartbeatTimeout(raftNode: A, previous: Option[CancelT]): CancelT

  def cancelTimeout(c: CancelT): Unit

}

object RaftTimer {

  def apply[A: TimerCallback](sendHeartbeatTimeout: FiniteDuration,
                              receiveHeartbeatTimeout: FiniteDuration,
                              schedulerService: ScheduledExecutorService = java.util.concurrent.Executors.newScheduledThreadPool(1),
                              cancelMayInterruptIfRunning: Boolean = true): RaftTimer[A] = {
    new RaftTimer[A] {
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
  }
}
