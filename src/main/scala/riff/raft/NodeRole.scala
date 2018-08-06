package riff.raft

sealed trait NodeRole
object NodeRole {

  case object Follower extends NodeRole

  case object Leader extends NodeRole

  case object Candidate extends NodeRole
}
