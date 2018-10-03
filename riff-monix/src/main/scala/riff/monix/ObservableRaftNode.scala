package riff.monix
import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler
import monix.reactive.observers.Subscriber
import monix.reactive.subjects.Var
import monix.reactive.{Observable, Observer}
import riff.raft.log.{LogAppendSuccess, RaftLog}
import riff.raft.messages.{AddressedMessage, AppendEntriesResponse, RaftMessage, ReceiveHeartbeatTimeout}
import riff.raft.node.{AddressedRequest, RaftMessageHandler, RaftNode}
import riff.raft.{AppendStatus, NodeId, RaftClient}

/**
  * Wraps a RaftNode's onMessage so as to exposes observable requests and responses
  *
  * @param node the wrapped RaftNode
  * @param sched a scheduler to use for the feeds
  * @tparam A the type of data which is appended to the log (could just be a byte array, some union type, etc)
  */
class ObservableRaftNode[A] private[monix] (underlying: RaftNode[A], input: Subscriber[RaftMessage[A]])(
  implicit sched: Scheduler)
    extends RaftMessageHandler[A] with StrictLogging {

  override def toString = underlying.toString()
  private val requestVar = Var[RaftMessage[A]](null)
  private val responseVar = Var[Result](null)

  def inputsObservable: Observable[RaftMessage[A]] = requestVar.filter(_ != null)

  def responsesAsObservable: Observable[Result] = responseVar.filter(_ != null)

  def role() = underlying.state().role
  def nodeId = underlying.nodeId
  def log: RaftLog[A] = underlying.log

  def resetReceiveHeartbeat(): Unit = {
//    underlying.resetReceiveHeartbeat()
    underlying.onMessage(ReceiveHeartbeatTimeout)
  }

  /**
    *
    * @param from the node from which this message is received
    * @param msg the Raft message
    * @return the response
    */
  override def onMessage(input: RaftMessage[A]): Result = {
    requestVar := input

    val res = underlying.onMessage(input)

    responseVar := res

    res
  }
}
