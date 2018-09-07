package riff.raft.node

sealed trait NodeRole
final case object Follower  extends NodeRole
final case object Leader    extends NodeRole
final case object Candidate extends NodeRole
