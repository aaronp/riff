package riff.raft.log
import riff.raft.{LogIndex, Term}

/**
  * Represents the return type of a file-based raft log
  *
  * @param written
  * @param replaced
  */
final case class LogAppendResult(term: Term, firstIndex: LogIndex, lastIndex: LogIndex, replacedIndices: Seq[LogIndex] = Nil)
