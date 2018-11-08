package riff.raft.node
import riff.raft.NodeId
import riff.raft.log.{LogAppendResult, LogCoords}
import riff.raft.messages.{AddressedMessage, RaftRequest, RaftResponse}

/**
  * Represents all possible results of a [[RaftNode]] having processed an event or message
  *
  * @tparam NodeKey the node type
  * @tparam A the log type
  */
sealed trait RaftNodeResult[+A] {

  /** @param targetNodeId
    * @return a representation of this result as a collection of [[AddressedMessage]]s which are either to or from this node
    */
  def toNode(targetNodeId : NodeId) : Seq[AddressedMessage[A]]
}

/**
  * Marker interface for a no-op result of a node having processed an event or message.
  *
  * At the moment there is just a 'log message' implementation, but we could make the results more strongly-typed
  * if needed (e.g. as a candidate advances through its votes, or the detail from some invalid action)
  */
sealed trait NoOpResult extends RaftNodeResult[Nothing]

object NoOpResult {
  case class LogMessageResult(msg: String) extends NoOpResult {
    override def toNode(targetNodeId: NodeId) = Nil
  }
  def apply(msg: String) = LogMessageResult(msg)
}

/** A sequence of requests to send out as a result of a [[RaftNode]] having processed an event or message
  *
  * @param requests the requests to send, coupled w/ the intended recipient
  * @tparam NodeKey the node type
  * @tparam A the log type
  */
final case class AddressedRequest[A](requests: Iterable[(NodeId, RaftRequest[A])]) extends RaftNodeResult[A] {
  def size = requests.size
  override def toNode(targetNodeId: NodeId) = {
    requests.collect {
      case (`targetNodeId`, msg) => AddressedMessage[A](targetNodeId, msg)
    }.toSeq
  }
}

object AddressedRequest {
  def apply[A](requests: (NodeId, RaftRequest[A])*): AddressedRequest[A] = new AddressedRequest(requests)
  def apply[A](key: NodeId, request: RaftRequest[A]): AddressedRequest[A] = new AddressedRequest(Iterable(key -> request))
}

/** Combines a 'replyTo' node with a response
  *
  * @param replyTo the node to which this response should be targeted
  * @param msg the response, presumably to a request sent from the 'replyTo' node
  * @tparam NodeKey the raft node
  */
final case class AddressedResponse(replyTo: NodeId, msg: RaftResponse) extends RaftNodeResult[Nothing] {
  override def toNode(targetNodeId: NodeId) = {
    if (replyTo == targetNodeId) {
      Seq(AddressedMessage(replyTo, msg))
    } else {
      Nil
    }
  }
}

/** This was introduced after having first just having:
  * 1) AddressedRequest
  * 2) AddressedResponse
  * 3) NoOpResult
  *
  * which seemed ok. Unfortunately we have situations (namely when the node is a leader) when we want more
  * information about e.g. commit/log append data so we can expose that data to a client.
  *
  * We could start introducing 'Either', 'Xor', etc types, but I'd like to avoid:
  * 1) the extra wrapping penalty
  * 2) overly-complex return types
  *
  * though the second is at the expense of scenarios which have to pattern-match on the result types
  *
  * @param committed if an [[riff.raft.messages.AppendEntriesResponse]] resulted in committing a log entry, these are the coordinates of those committed entries
  * @param response either a no-op (e.g. log) result or potentially an [[AddressedRequest]] to send the next data required
  * @tparam A the log type
  */
final case class LeaderCommittedResult[A](committed: Seq[LogCoords], response: RaftNodeResult[A]) extends RaftNodeResult[A] {
  override def toNode(targetNodeId: NodeId): Seq[AddressedMessage[A]] = response.toNode(targetNodeId)
}

/**
  * Like [[LeaderCommittedResult]] this was added so we could explicitly expose the extra information when appending data as a leader
  * as a result of an [[riff.raft.messages.AppendEntries]] request.
  *
  * @param appendResult result of appending some data to this node
  * @param request the request(s) (as wrapped in a AddressedRequest) to send
  * @tparam A the log type
  */
final case class NodeAppendResult[A](appendResult: LogAppendResult, request: AddressedRequest[A]) extends RaftNodeResult[A] {
  override def toNode(targetNodeId: NodeId): Seq[AddressedMessage[A]] = {
    request.toNode(targetNodeId)
  }
}
