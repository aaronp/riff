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
  def contains(key: NodeKey): Boolean
  def numberOfPeers: Int = peers.size
}

object RaftCluster {
  def apply[NodeKey](peers: Iterable[NodeKey]): Fixed[NodeKey] = new Fixed(peers.toSet)

  def apply[NodeKey](first: NodeKey, theRest: NodeKey*): Fixed[NodeKey] = apply(theRest.toSet + first)

  class Fixed[NodeKey](override val peers: Set[NodeKey]) extends RaftCluster[NodeKey] {
    override val numberOfPeers                   = peers.size
    override def contains(key: NodeKey): Boolean = peers.contains(key)
    override lazy val toString = {
      peers.toList.map(_.toString).sorted.mkString(s"${numberOfPeers + 1} node cluster (this node plus ${numberOfPeers} peers: [", ",", "])")
    }
  }
}
