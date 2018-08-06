package riff.raft.election

case class RequestVoteReply(term: Int, granted: Boolean)

object RequestVoteReply {
//  def apply[T: HasInfoForVote](receivingRaftNode: T, forRequest: RequestVoteData): RequestVoteReply = {
//    import HasInfoForVote.ops._
//    receivingRaftNode.replyTo(forRequest)
//  }
}
