package riff.monix

import monix.reactive.Observable
import monix.reactive.observers.Subscriber
import riff.monix.log.ObservableLog
import riff.monix.log.ObservableLog._
import riff.raft.RaftClient
import riff.raft.log.RaftLog
import riff.raft.messages.{AddressedMessage, RaftMessage}
import riff.raft.node._

/** Contains all the constituent parts use to construct a [[RaftNode]].
  *
  * This exposes observable behaviour for timeouts, role changes, log events
  *
  * @param raftNode the underlying raft node. This should NOT be used directly, but rather driven by the pipe
  * @param pipe the inputs/outputs used to drive this node
  * @param out the mapped output
  * @param timeouts a means to observe the node's timeouts
  * @param roleObs
  * @tparam A
  */
class MonixNode[A] private[monix] (
  val raftNode: ObservableRaftNode[A],
  val pipe: NamedPipe[RaftMessage[A], RaftMessage[A]],
  val out: Observable[AddressedMessage[A]],
  val timeouts: ObservableCallback,
  val roleObservable: NodeRoleObservable) {

  override def toString = {
    s"MonixNode $nodeId: $raftNode"
  }
  override def hashCode = nodeId.hashCode * 37
  override def equals(other: Any) = {
    other match {
      case n: MonixNode[_] => nodeId == n.nodeId
      case _ => false
    }
  }

  def observableLog(): Option[ObservableLog[A]] = log().asObservable

  def log(): RaftLog[A] = raftNode.log

  def isLeader() = raftNode.role() == Leader

  def nodeId = raftNode.nodeId

  def resetReceiveHeartbeat(): Unit = {
    raftNode.resetReceiveHeartbeat()
  }

  def client: RaftClient[Observable, A] = raftNode

  /** @return a thread-safe concurrent subscription which can be used to accept inputs
    */
  def bufferedSubscriber: Subscriber[RaftMessage[A]] = pipe.bufferedSubscriber
}
