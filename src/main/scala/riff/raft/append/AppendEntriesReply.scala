package riff.raft.append

/**
  * @param term this node's term
  * @param success whether the append was successful
  * @param matchIndex the lastest log index which agrees with the leader node's, or 0 if unknown
  */
final case class AppendEntriesReply(term: Int, success: Boolean, matchIndex: Int)
