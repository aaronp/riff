package riff.raft.log
import riff.raft.{LogIndex, Term}

/**
  * Wraps a log by keeping in memory the latest commit/append index
  *
  * @param underlying the underlying log
  * @tparam A
  */
class CachingLog[A](val underlying: RaftLog[A]) extends RaftLog[A] {
  private var latestAppendCache: Option[LogCoords] = None
  private var latestCommitCache: Option[LogIndex]  = None
  override def appendAll(coords: LogCoords, data: Array[LogEntry[A]]): LogAppendResult = {
    latestAppendCache = None
    underlying.appendAll(coords, data)
  }
  override def latestCommit(): LogIndex = {
    latestCommitCache.getOrElse {
      val value = underlying.latestCommit()
      latestCommitCache = Option(value)
      value
    }
  }
  override def termForIndex(index: LogIndex): Option[Term] = {
    underlying.termForIndex(index)
  }
  override def latestAppended(): LogCoords = {
    latestAppendCache.getOrElse {
      val value = underlying.latestAppended
      latestAppendCache = Option(value)
      value
    }
  }
  override def commit(index: LogIndex): Seq[LogCoords] = {
    latestCommitCache = None
    underlying.commit(index)
  }
  override def entryForIndex(index: LogIndex) = underlying.entryForIndex(index)
}
