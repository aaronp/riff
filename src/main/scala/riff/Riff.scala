package riff

import monix.execution.{Cancelable, Scheduler}
import org.reactivestreams.Publisher
import riff.impl.RaftClusterClient

import scala.concurrent.duration.FiniteDuration

/**
  * The external use-cases for a node in the Raft protocol are:
  *
  * 1) adding/removing nodes
  * 2) appending data to the cluster, which should return either a new (pending) commit coords, a redirect, or a 'no known leader'
  * 3) subscribing to lifecycle events, which should return a Publisher which lets subscribers know about cluster and leadership changes
  * 4) subscribing to a commit log, which should return a Publisher which lets subscribers know when logs are appended or committed
  *
  * All of these algebras don't necessarily have to be in the same trait
  */
// https://softwaremill.com/free-tagless-compared-how-not-to-commit-to-monad-too-early/
trait DataSync[A] {
  def append(data: A): Publisher[AppendResult]
}

/**
  * Represents the ability to add and remove members from the cluster dynamically.
  *
  * Clusters should ideally be static, so this functionality is purely optional.
  *
  * The implied behaviour is that the implementation will know how to implement [[RaftClusterClient]]
  */
trait ClusterAdmin {
  def addNode(name: String): Boolean

  def removeNode(name: String): Boolean
}

/**
  * Represents a node in a Raft cluster
  */
trait ClusterNode {

  /**
    *
    * @return a stream of ClusterEvents (nodes added, removed)
    */
  def membership: Publisher[ClusterEvent]

  /**
    * This function is intended to be invoked periodically be a timer when the node is the leader. If the node is NOT the leader, calling this
    * function should have no effect
    *
    * @return true if the call had an effect (i.e., we are the leader), false otherwise
    */
  def onSendHeartbeatTimeout(): Boolean

  /**
    * This function is intended to be invoked periodically be a timer when the node is a follower. If the node is NOT a follower, calling this
    * function should have no effect
    *
    * @return true if the call had an effect (i.e., we are a follower), false otherwise
    */
  def onReceiveHeartbeatTimeout(): Boolean
}

trait RaftTimer {

  type C

  /**
    * Resets the heartbeat timeout for the given node.
    *
    * It contract is assumed that this function will be called periodically from the node passed in,
    * and it is up to the implementation to invoking 'oElectionTimeout' should it not be invoked
    * within a certain time
    *
    * @param node
    */
  def resetReceiveHeartbeatTimeout(node: ClusterNode, previous: Option[C]): C

  def resetSendHeartbeatTimeout(node: ClusterNode, previous: Option[C]): C

  def cancelTimeout(c: C): Unit

}

object RaftTimer {
  def apply(sendHeartbeatTimeout: FiniteDuration, receiveHeartbeatTimeout: FiniteDuration)(implicit sched: Scheduler): RaftTimer = {
    new RaftTimer {
      type C = Cancelable

      override def cancelTimeout(c: Cancelable): Unit = c.cancel()

      override def resetSendHeartbeatTimeout(node: ClusterNode, previous: Option[C]) = {
        previous.foreach(_.cancel())
        val cancel: Cancelable = sched.scheduleOnce(sendHeartbeatTimeout) {
          node.onSendHeartbeatTimeout()
        }
        cancel
      }

      override def resetReceiveHeartbeatTimeout(node: ClusterNode, previous: Option[Cancelable]): Cancelable = {
        previous.foreach(_.cancel())
        val cancel: Cancelable = sched.scheduleOnce(receiveHeartbeatTimeout) {
          node.onReceiveHeartbeatTimeout()
        }
        cancel
      }
    }
  }
}

trait Riff[A] extends DataSync[A] with ClusterAdmin with ClusterNode

object Riff {}
