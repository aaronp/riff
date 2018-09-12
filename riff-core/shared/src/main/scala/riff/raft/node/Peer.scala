package riff.raft.node

import riff.raft.LogIndex

/**
  * The view of a peer in the cluster
  *
  * @param nextIndex the latest index required by the peer as understood by the leader. This can be updated/maintained w/o consulting the node, but rather immediately upon the leader receiving an append request.
  * @param matchIndex set to zero when the state of the peer is unknown, otherwise the latest known matching log index of the peer
  */
class Peer private (val nextIndex: LogIndex, val matchIndex: LogIndex) {
  require(matchIndex <= nextIndex, s"Match index '$matchIndex' should always be less than next index '$nextIndex'")
  require(nextIndex > 0)
  require(matchIndex >= 0)
  override def toString = s"Peer(nextIndex=$nextIndex, matchIndex=$matchIndex)"

  override def equals(other: Any) = other match {
    case Peer(`nextIndex`, `matchIndex`) => true
    case _                               => false
  }

  override def hashCode(): Int                      = (31 * nextIndex) + matchIndex
  def setUnmatchedNextIndex(newNextIndex: LogIndex) = new Peer(newNextIndex, matchIndex = 0)
  def setMatchIndex(index: LogIndex)                = new Peer(index + 1, index)
}

object Peer {

  def unapply(peer: Peer): Option[(LogIndex, LogIndex)] = Option(peer.nextIndex, peer.matchIndex)

  def withUnmatchedNextIndex(nextIndex: LogIndex): Peer = new Peer(nextIndex, 0)

  def withMatchIndex(initialIndex: LogIndex): Peer = {
    new Peer(nextIndex = initialIndex + 1, matchIndex = initialIndex)
  }

  val Empty = withMatchIndex(0)
}
