package riff.raft.node
import riff.raft.{LogIndex, NodeId}
import riff.raft.log.LogCoords
import riff.raft.messages.AppendEntriesResponse

import scala.collection.immutable

/**
  * Keeps track of the leader's ephemeral view of the cluster
  *
  * @param initialPeersByKey the known cluster state
  * @tparam NodeId the type representing this peer node. Typically just a String identifier,
  *                 though could be something more meaningful/useful, like a websocket. Just so long as it provides a meaningful hashCode/equals
  */
private[node] class LeadersClusterView(cluster: RaftCluster) {
  private var peersByKey = Map[NodeId, Peer]()

  def eligibleNodesForPreviousEntry(previous: LogCoords): immutable.Iterable[NodeId] = {
    toMap.collect {
      case (key, peer) if peer.matchIndex == previous.index => key
    }
  }

  /** @param index
    * @return the number of nodes which have AT LEAST the same match index
    */
  def matchIndexCount(index: LogIndex): Int = {
    cluster.peers.count { id => //
      peersByKey.get(id).map(_.matchIndex).exists(_ >= index)
    }
  }

  def toMap(): Map[NodeId, Peer] = cluster.peers.foldLeft(Map[NodeId, Peer]()) {
    case (map, id) => map.updated(id, peersByKey.getOrElse(id, Peer.Empty))
  }

  def stateForPeer(peer: NodeId): Option[Peer] =
    if (cluster.contains(peer)) {
      peersByKey.get(peer).orElse(Option(Peer.Empty))
    } else {
      None
    }

  def update(node: NodeId, response: AppendEntriesResponse): Option[Peer] = {
    if (!cluster.contains(node)) {
      peersByKey = peersByKey - node
      None
    } else {
      val oldPeer = peersByKey.getOrElse(node, Peer.Empty)
      val newPeer = if (response.success) {
        oldPeer.setMatchIndex(response.matchIndex)
      } else {
        val newNextIndex = oldPeer.nextIndex - 1
        if (newNextIndex > 0) {
          oldPeer.setUnmatchedNextIndex(newNextIndex)
        } else {
          Peer.Empty
        }
      }
      update(node, newPeer)
      Option(newPeer)
    }
  }

  private def update(key: NodeId, peer: Peer) = {
    peersByKey = peersByKey.updated(key, peer)
  }

  def numberOfPeers(): Int = cluster.numberOfPeers

  override def toString(): String = {
    val map = toMap
    map.mkString(s"clusterView of ${map.size} nodes: {", ";", "}")
  }
}

object LeadersClusterView {

  def apply(keys: NodeId*): LeadersClusterView = apply(RaftCluster(keys.toIterable))

  def apply(first: (NodeId, Peer), theRest: (NodeId, Peer)*): LeadersClusterView = {
    val view = LeadersClusterView(RaftCluster(first._1, theRest.map(_._1): _*))
    view.update(first._1, first._2)
    theRest.foreach {
      case (node, p) => view.update(node, p)
    }
    view
  }

  def apply(cluster: RaftCluster): LeadersClusterView = {
    new LeadersClusterView(cluster)
  }
}
