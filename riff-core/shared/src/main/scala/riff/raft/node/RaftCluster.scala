package riff.raft.node
import riff.raft.NodeId

/**
  *  Represents something which knows about the peers in the cluster.
  *  This is typically (and recommended to be) a fixed size cluster.
  *
  *  The NodeKey themselves are often just String identifiers used as a look-up for a means of communicating
  *  with that peer, but could be the peer transport itself, provided it has a good hashCode/equals
  *
  * @tparam NodeKey the type of peer node
  */
trait RaftCluster {

  def asDynamicCluster(): RaftCluster.Dynamic = {
    this match {
      case d: RaftCluster.Dynamic => d
      case _                               => RaftCluster.dynamic(peers.toSet)
    }
  }

  def peers: Iterable[NodeId]
  def contains(key: NodeId): Boolean
  def numberOfPeers: Int = peers.size
}

object RaftCluster {
  def apply(peers: Iterable[NodeId]): Fixed = new Fixed(peers.toSet)

  def apply(first: NodeId, theRest: NodeId*): Fixed = apply(theRest.toSet + first)

  def dynamic(nodes: NodeId*): Dynamic     = dynamic(nodes.toSet)
  def dynamic(nodes: Set[NodeId]): Dynamic = new Dynamic(nodes)

  class Dynamic(initialPeers: Set[NodeId]) extends RaftCluster {
    private var nodePeers = initialPeers
    def add(peer: NodeId) = {
      nodePeers = nodePeers + peer
    }
    def remove(peer: NodeId) = {
      nodePeers = nodePeers - peer
    }
    override def peers: Iterable[NodeId]        = nodePeers
    override def contains(key: NodeId): Boolean = nodePeers.contains(key)
  }

  class Fixed(override val peers: Set[NodeId]) extends RaftCluster {
    override val numberOfPeers                   = peers.size
    override def contains(key: NodeId): Boolean = peers.contains(key)
    override lazy val toString = {
      peers.toList.map(_.toString).sorted.mkString(s"${numberOfPeers + 1} node cluster (this node plus ${numberOfPeers} peers: [", ",", "])")
    }
  }
}
