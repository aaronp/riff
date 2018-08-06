package riff.raft.append

sealed trait AppendResult

object AppendResult {
  def becomeFollowerInNewTerm(term: Int, reply: AppendEntriesReply) = BecomeFollowerInNewTerm(term, reply)
}

case class BecomeFollowerInNewTerm(term: Int, reply: AppendEntriesReply) extends AppendResult
