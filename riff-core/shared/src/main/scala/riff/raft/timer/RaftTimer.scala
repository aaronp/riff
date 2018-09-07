package riff.raft.timer

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

  def apply[A: TimerCallback](sendHeartbeatTimeout: FiniteDuration, receiveHeartbeatTimeout: FiniteDuration) = {
    DefaultTimer[A](sendHeartbeatTimeout, receiveHeartbeatTimeout)
  }
}
