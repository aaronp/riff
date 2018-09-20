package riff.raft.timer

import scala.concurrent.duration.FiniteDuration

/**
  * Represents the functions required to control a member node's election and heartbeat timeouts.
  *
  * Implementations should introduce an element of randomisation when resetting the receive heartbeat timeouts,
  * presumably based on some configuration, in order to follow the Raft spec.
  *
  * The intention being to reduce the likelihood of multiple nodes becoming candidates at the same time.
  *
  */
trait RaftTimer[A] {

  /**
    * Some token which can be used to cancel an existing timeout
    */
  type CancelT

  /**
    * Resets the heartbeat timeout for the given node.
    *
    * It is assumed that this function will be called periodically from the node passed in,
    * and it is up to the implementation trigger an election timeout on the node should it not be reset or cancelled
    * within a certain (presumably randomized) time.
    *
    * @param raftNode the node which should be timed-out should another 'reset 'or 'cancelTimeout' be called on the returned value
    * @param previous An optional previous cancelation token to cancel
    */
  def resetReceiveHeartbeatTimeout(raftNode: A, previous: Option[CancelT]): CancelT

  /**
    * Resets a leader's send heartbeat timeout for a given node.
    *
    * It is assumed that this function will be called periodically from the node passed in order to send a heartbeat
    * to the given 'state'
    *
    * @param raftNode the node which should be timed-out should another 'reset 'or 'cancelTimeout' be called on the returned value
    * @param previous An optional previous cancelation token to cancel
    */
  def resetSendHeartbeatTimeout(raftNode: A, previous: Option[CancelT]): CancelT

  /** @param c the token to cancel
    */
  def cancelTimeout(c: CancelT): Unit

}

object RaftTimer {

  def apply[A: TimerCallback](sendHeartbeatTimeout: FiniteDuration, receiveHeartbeatTimeout: FiniteDuration) = {
    DefaultTimer[A](sendHeartbeatTimeout, receiveHeartbeatTimeout)
  }
}
