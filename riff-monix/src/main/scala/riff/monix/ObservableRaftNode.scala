package riff.monix
import monix.execution.Scheduler
import monix.reactive.observers.Subscriber
import monix.reactive.subjects.Var
import monix.reactive.{Observable, Observer, Pipe}
import riff.raft.log.{LogAppendResult, LogAppendSuccess, RaftLog}
import riff.raft.messages.{AddressedMessage, AppendEntriesResponse, RaftMessage}
import riff.raft.node.{AddressedRequest, AddressedResponse, RaftMessageHandler, RaftNode}
import riff.raft.{AppendStatus, NodeId, NotTheLeaderException, RaftClient}

/**
  * Wraps a RaftNode's onMessage so as to exposes observable requests and responses
  *
  * @param node the wrapped RaftNode
  * @param sched a scheduler to use for the feeds
  * @tparam A the type of data which is appended to the log (could just be a byte array, some union type, etc)
  */
class ObservableRaftNode[A] private[monix] (underlying: RaftNode[A], input: Subscriber[RaftMessage[A]])(
  implicit sched: Scheduler)
    extends RaftMessageHandler[A] with RaftClient[Observable, A] {

  override def toString = underlying.toString()
//  private val (requestFeed, requestObs) = Pipe.replayLimited[RaftMessage[A]](1).multicast
//  private val (responseFeed, responseObs) = Pipe.replayLimited[Result](1).multicast
  private val requestVar = Var[RaftMessage[A]](null)
  private val responseVar = Var[Result](null)

  def requestsAsObservable: Observable[RaftMessage[A]] = requestVar.filter(_ != null)

  def responsesAsObservable: Observable[Result] = responseVar.filter(_ != null)

  def role() = underlying.state().role
  def nodeId = underlying.nodeId
  def log: RaftLog[A] = underlying.log

  def resetReceiveHeartbeat(): Unit = {
    underlying.resetReceiveHeartbeat()
  }

  /**
    * This is a client's view of a raft cluster, which simply wants to write some data
    *
    * @param data the data to write
    * @return an observable of the append results as they are appended/co
    */
  override def append(data: Array[A]): Observable[AppendStatus] = {

    appendIfLeader(data) match {
      case None =>
        val st8 = underlying.state()
        Observable.raiseError(new NotTheLeaderException(st8.id, underlying.currentTerm, st8.leader))
      case Some((error: Exception, _)) => Observable.raiseError(error)
      case Some((success: LogAppendSuccess, requests: AddressedRequest[A])) =>
        val responses: Observable[(NodeId, AppendEntriesResponse)] =
          responsesAsObservable.dump("client response").collect {
            case AddressedResponse(from, resp: AppendEntriesResponse) if success.contains(resp) => (from, resp)
          }

        val committed = requests match {
          // if we're a cluster of 1, then the entry will already have been committed. We
          case AddressedRequest(empty) if empty.isEmpty =>
            assert(underlying.log.latestCommit() == success.lastIndex.index)
            true
          case _ => false
        }

        // some client of this node has just invoked 'append'.
        // The result contains the requests which need to be sent to
        // the rest of the cluster.
        //
        // The whole node is represented as a single pipe of RaftMessage[A] ==[RaftNode]==> RaftNodeResult[A]
        //
        // the provided 'input' allows us to contribute to that stream of inputs
        //
        val messages = requests.requests.map {
          case (to, msg) => AddressedMessage(to, msg)
        }
        Observer.feed(input, messages)

        //
        // return the observable results
        //
        val clusterSize = underlying.cluster.numberOfPeers + 1
        responses
          .scan(AppendStatus(success, Map(nodeId -> true), committed, clusterSize)) {
            case (status, (fromId, appendResponse)) => status.withResult(fromId, appendResponse.success)
          }
          .take(clusterSize)
    }
  }

  override def createAppend(data: Array[A]): Result = {
    underlying.createAppend(data)
  }

  def appendIfLeader(data: Array[A]): Option[(LogAppendResult, AddressedRequest[A])] = {
    underlying.appendIfLeader(data)
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
