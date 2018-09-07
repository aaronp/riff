package riff.raft.node

import riff.raft.LogIndex

/**
  * The view of a peer in the cluster
  *
  * @param name
  * @param nextIndex the latest index required by the peer as understood by the leader. This can be updated/maintained w/o consulting the node, but rather immediately upon the leader receiving an append request.
  * @param matchIndex
  * @param voteGranted
  * @param lastRpcSent
  * @param lastHeartbeatSent
  */
case class Peer(nextIndex: LogIndex, matchIndex: LogIndex) {
  require(matchIndex < nextIndex, s"Match index '$matchIndex' should always be less than next index '$nextIndex'")
  require(nextIndex > 0)
  require(matchIndex >= 0)
}

object Peer {

  def apply(): Peer = {
    Peer(nextIndex = 1, matchIndex = 0)
  }
}
