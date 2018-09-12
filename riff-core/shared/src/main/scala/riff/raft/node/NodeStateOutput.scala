package riff.raft.node
import riff.raft.messages.{RaftRequest, RaftResponse}

/**
  * Represents all possible results of a [[NodeState]] having processed an event or message
  *
  * @tparam NodeKey the node type
  * @tparam A the log type
  */
sealed trait NodeStateOutput[+NodeKey, +A]

/**
  * Marker interface for a no-op result of a node having processed an event or message.
  *
  * At the moment there is just a 'log message' implementation, but we could make the results more strongly-typed
  * if needed (e.g. as a candidate advances through its votes, or the detail from some invalid action)
  */
sealed trait NoOpOutput extends NodeStateOutput[Nothing, Nothing]

object NoOpOutput {
  case class LogMessageOutput(msg: String) extends NoOpOutput
  def apply(msg: String) = LogMessageOutput(msg)
}

/** A sequence of requests to send out as a result of a [[NodeState]] having processed an event or message
  *
  * @param requests the requests to send
  * @tparam NodeKey the node type
  * @tparam A the log type
  */
final case class AddressedRequest[NodeKey, A](requests: Iterable[(NodeKey, RaftRequest[A])]) extends NodeStateOutput[NodeKey, A]

object AddressedRequest {
  def apply[NodeKey, A](requests: (NodeKey, RaftRequest[A])*) = {
    new AddressedRequest(requests)
  }
  def apply[NodeKey, A](key: NodeKey, request: RaftRequest[A]) = {
    new AddressedRequest(Iterable(key -> request))
  }
}

/** Combines a 'replyTo' node with a response
  *
  * @param replyTo the node to which this response should be targeted
  * @param msg the response, presumably to a request sent from the 'replyTo' node
  * @tparam NodeKey the raft node
  */
final case class AddressedResponse[NodeKey](replyTo: NodeKey, msg: RaftResponse) extends NodeStateOutput[NodeKey, Nothing]
