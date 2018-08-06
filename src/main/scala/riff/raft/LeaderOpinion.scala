package riff.raft

import riff.RedirectException

/**
  * Represents what a node currently things the leader is.
  *
  */
sealed trait LeaderOpinion {
  def name: String
  def term: Int
  def isLeader = false
  def asError = new RedirectException(this)
}

object LeaderOpinion {

  /** An initial state - whenever a follower hasn't received a Request
    *
    * @param name the current node's name
    * @param term
    */
  final case class Unknown(name: String, term: Int) extends LeaderOpinion

  /** When a node becomes a candidate
    *
    * @param name the leader name (e.g the node's name which has this opinion'
    * @param term the term
    */
  final case class ImACandidate(name: String, term: Int) extends LeaderOpinion

  /** When a node becomes the leader
    *
    * @param name the leader name (e.g the node's name which has this opinion'
    * @param term the term
    */
  final case class IAmTheLeader(name: String, term: Int) extends LeaderOpinion {
    override val isLeader = true
  }

  /** When a follower __votes_for__ another node. That node may not actually get elected,
    * mind.
    *
    * @param name the leader name
    * @param term the term
    */
  final case class TheLeaderIs(name: String, term: Int) extends LeaderOpinion

}
