package riff.raft.node
import riff.raft.messages.{RaftRequest, RaftResponse}

sealed trait NodeStateOutput[+NodeKey, +A]

sealed trait NoOpOutput extends NodeStateOutput[Nothing, Nothing]
object NoOpOutput {
  case class LogMessageOutput(msg: String) extends NoOpOutput
  def apply(msg: String) = LogMessageOutput(msg)
}

final case class AddressedRequest[NodeKey, A](requests: Iterable[(NodeKey, RaftRequest[A])]) extends NodeStateOutput[NodeKey, A] {
  def nodes: Iterable[NodeKey] = requests.map(_._1)
}
object AddressedRequest {
  def apply[NodeKey, A](requests: (NodeKey, RaftRequest[A])*) = {
    new AddressedRequest(requests)
  }
  def apply[NodeKey, A](key: NodeKey, request: RaftRequest[A]) = {
    new AddressedRequest(Iterable(key -> request))
  }
}
final case class AddressedResponse[NodeKey](node: NodeKey, msg: RaftResponse) extends NodeStateOutput[NodeKey, Nothing]
