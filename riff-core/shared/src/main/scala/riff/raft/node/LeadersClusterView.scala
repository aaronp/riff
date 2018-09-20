package riff.raft.node
import riff.raft.LogIndex
import riff.raft.log.LogCoords
import riff.raft.messages.AppendEntriesResponse

import scala.collection.immutable

/**
  * Keeps track of the leader's ephemeral view of the cluster
  *
  * @param initialPeersByKey the known cluster state
  * @tparam NodeKey the type representing this peer node. Typically just a String identifier,
  *                 though could be something more meaningful/useful, like a websocket. Just so long as it provides a meaningful hashCode/equals
  */
private[node] class LeadersClusterView[NodeKey](cluster: RaftCluster[NodeKey]) {
  private var peersByKey = Map[NodeKey, Peer]()

  def eligibleNodesForPreviousEntry(previous: LogCoords): immutable.Iterable[NodeKey] = {
    toMap.collect {
      case (key, peer) if peer.matchIndex == previous.index => key
    }
  }

  /** @param index
    * @return the number of nodes which have AT LEAST the same match index
    */
  def matchIndexCount(index: LogIndex): Int = {
    cluster.peers.count { id => peersByKey.get(id).map(_.matchIndex).exists(_ >= index)
    }
  }

  def toMap(): Map[NodeKey, Peer] = cluster.peers.foldLeft(Map[NodeKey, Peer]()) {
    case (map, id) => map.updated(id, peersByKey.getOrElse(id, Peer.Empty))
  }

  def stateForPeer(peer: NodeKey): Option[Peer] =
    if (cluster.contains(peer)) {
      peersByKey.get(peer).orElse(Option(Peer.Empty))
    } else {
      None
    }

  def update(node: NodeKey, response: AppendEntriesResponse): Option[Peer] = {
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

  private def update(key: NodeKey, peer: Peer) = {
    peersByKey = peersByKey.updated(key, peer)
  }

  def numberOfPeers(): Int = cluster.numberOfPeers

  override def toString(): String = {
    val map = toMap
    map.mkString(s"clusterView of ${map.size} nodes: {", ";", "}")
  }
}

object LeadersClusterView {

  def apply[NodeKey](keys: NodeKey*): LeadersClusterView[NodeKey] = apply(RaftCluster(keys.toIterable))

  def apply[NodeKey](first: (NodeKey, Peer), theRest: (NodeKey, Peer)*): LeadersClusterView[NodeKey] = {
    val view = LeadersClusterView(RaftCluster[NodeKey](first._1, theRest.map(_._1): _*))
    view.update(first._1, first._2)
    theRest.foreach {
      case (node, p) => view.update(node, p)
    }
    view
  }

  def apply[NodeKey](cluster: RaftCluster[NodeKey]): LeadersClusterView[NodeKey] = {
    new LeadersClusterView[NodeKey](cluster)
  }
}
