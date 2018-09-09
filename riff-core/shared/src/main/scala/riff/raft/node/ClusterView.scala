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
class ClusterView[NodeKey](initialPeersByKey: Map[NodeKey, Peer]) {

  def nodesMatching(previous: LogCoords): immutable.Iterable[NodeKey] = {
    peersByKey.collect {
      case (key, peer) if peer.matchIndex == previous.index => key
    }
  }

  private var peersByKey = initialPeersByKey

  def toMap(): Map[NodeKey, Peer] = peersByKey

  def stateForPeer(peer: NodeKey): Option[Peer] = peersByKey.get(peer)

  def update(node: NodeKey, response: AppendEntriesResponse): Option[Peer] = {
    stateForPeer(node).map { oldPeer =>
      val newPeer = if (response.success) {
        oldPeer.copy(nextIndex = response.matchIndex + 1, matchIndex = response.matchIndex)
      } else {
        val newNextIndex = oldPeer.nextIndex - 1
        if (newNextIndex > 0) {
          oldPeer.copy(nextIndex = newNextIndex, matchIndex = 0)
        } else {
          Peer()
        }
      }
      peersByKey = peersByKey.updated(node, newPeer)
      newPeer
    }
  }

  def numberOfPeers: Int = peersByKey.size

  /** @param index
    * @return the number of nodes which have AT LEAST the same match index
    */
  def matchIndexCount(index: LogIndex): Int = peersByKey.values.count(_.matchIndex >= index)

  override def toString = peersByKey.mkString(s"clusterView of ${peersByKey.size} nodes: {", ";", "}")
}

object ClusterView {
  def apply[NodeKey](initialIndex: LogIndex, cluster: RaftCluster[NodeKey]): ClusterView[NodeKey] = {
    apply(initialIndex, cluster.peers)
  }

  def apply[NodeKey](initialIndex: LogIndex, keys: NodeKey*): ClusterView[NodeKey] = {
    apply(initialIndex, keys.toIterable)
  }

  def apply[NodeKey](first: (NodeKey, Peer), theRest: (NodeKey, Peer)*): ClusterView[NodeKey] = {
    new ClusterView((first +: theRest).toMap.ensuring(_.size == 1 + theRest.size))
  }

  def apply[NodeKey](initialIndex: LogIndex, cluster: Iterable[NodeKey]): ClusterView[NodeKey] = {
    val peersByKey = cluster.foldLeft(Map[NodeKey, Peer]()) {
      case (map, key) =>
        map.get(key).foreach { duplicate => throw new IllegalStateException(s"Duplicate node peers found: $key and $duplicate")
        }
        map.updated(key, Peer(initialIndex))
    }
    new ClusterView[NodeKey](peersByKey)
  }
}
