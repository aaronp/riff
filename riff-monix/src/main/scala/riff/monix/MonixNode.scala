package riff.monix

import monix.execution.Scheduler
import monix.reactive.Observable
import riff.monix.log.ObservableLog
import riff.monix.log.ObservableLog._
import riff.raft.RaftClient
import riff.raft.log.RaftLog
import riff.raft.messages.{AddressedMessage, RaftMessage, ReceiveHeartbeatTimeout}
import riff.raft.node._

object MonixNode {

  def apply[A](wrappedNode: RaftNode[A], pipe: NamedPipe[RaftMessage[A], RaftMessage[A]])(implicit sched: Scheduler): MonixNode[A] = {

    // use a different callback other than the node itself
    val callback = new ObservableCallback
    callback.sendTimeout.subscribe(pipe.bufferedSubscriber)
    callback.receiveTimeouts.subscribe(pipe.bufferedSubscriber)

    // also attach an 'appendIfLeader' input

    val roleObs = ObservableState()
    val raftNode: ObservableRaftNode[A] = {
      val node = wrappedNode //
        .withTimerCallback(callback) //
        .withRoleCallback(roleObs)

      new ObservableRaftNode[A](node, pipe.bufferedSubscriber)
    }

    val out = pipe.output
    //.dump(s"${raftNode.nodeId} receiving")
      .map(raftNode.onMessage)
      //.dump(s"${raftNode.nodeId} sending")
      .flatMap(resultAsObservable)
    new MonixNode[A](raftNode, out, callback, roleObs)
  }
}

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
  val out: Observable[AddressedMessage[A]],
  val timeouts: ObservableCallback,
  stateCallback: ObservableState) {

  def observableState: Observable[RoleCallback.RoleEvent] = stateCallback.asObservable

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
    raftNode.onMessage(ReceiveHeartbeatTimeout)
  }

  def client: RaftClient[Observable, A] = ??? //raftNode

}
