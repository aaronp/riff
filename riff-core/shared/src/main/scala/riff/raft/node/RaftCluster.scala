package riff.raft.node

/**
  *  Represents something which knows about the peers in the cluster.
  *  This is typically (and recommended to be) a fixed size cluster.
  *
  *  The NodeKey themselves are often just String identifiers used as a look-up for a means of communicating
  *  with that peer, but could be the peer transport itself, provided it has a good hashCode/equals
  *
  * @tparam NodeKey the type of peer node
  */
trait RaftCluster[NodeKey] {
  def peers: Iterable[NodeKey]
  def size = peers.size
}

object RaftCluster {
  def apply[NodeKey](peers: Iterable[NodeKey]): Fixed[NodeKey] = new Fixed(peers)

  def apply[NodeKey](first: NodeKey, theRest: NodeKey*): Fixed[NodeKey] = apply(first :: theRest.toList)

  class Fixed[NodeKey](override val peers: Iterable[NodeKey]) extends RaftCluster[NodeKey] {
    override def toString = peers.mkString(s"${peers.size} node cluster [", ",", "]")
  }
}
