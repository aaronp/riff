package riff.raft.election

/**
  * @param term
  * @param lastLogIndex
  * @param lastLogTerm
  */
final case class RequestVoteData(term: Int, lastLogIndex: Int, lastLogTerm: Int)
