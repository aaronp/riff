package riff.raft.log

import riff.raft.{LogIndex, Term}

trait DelegateLog[A] extends RaftLog[A] {
  def underlying: RaftLog[A]
  override def appendAll(fromIndex: LogIndex, data: Array[LogEntry[A]]): LogAppendResult = underlying.appendAll(fromIndex, data)
  override def latestCommit(): LogIndex                                                  = underlying.latestCommit()
  override def termForIndex(index: LogIndex): Option[Term]                               = underlying.termForIndex(index)
  override def latestAppended(): LogCoords                                               = underlying.latestAppended()
  override def commit(index: LogIndex): Seq[LogCoords]                                   = underlying.commit(index)
  override def entryForIndex(index: LogIndex): Option[LogEntry[A]]                       = underlying.entryForIndex(index)
}
