package riff

/**
  * Represents an event in the Raft cluster
  */
sealed trait ClusterEvent

object ClusterEvent {
  def nodeAdded(name: String): NodeAdded = NodeAdded(name)

  def nodeRemoved(name: String): NodeRemoved = NodeRemoved(name)

  def leaderChanged(from: Option[String], to: String) = LeaderChange(from, to)
}

final case class NodeAdded(name: String) extends ClusterEvent

final case class NodeRemoved(name: String) extends ClusterEvent

final case class LeaderChange(from: Option[String], to: String) extends ClusterEvent
