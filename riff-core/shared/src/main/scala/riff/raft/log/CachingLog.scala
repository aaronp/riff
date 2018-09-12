package riff.raft.log
import riff.raft.LogIndex

/**
  * Wraps a log by keeping in memory the latest commit/append index
  *
  * @param underlying the underlying log
  * @tparam A
  */
class CachingLog[A](override val underlying: RaftLog[A]) extends DelegateLog[A] {
  private var latestAppendCache: Option[LogCoords] = None
  private var latestCommitCache: Option[LogIndex]  = None
  override def appendAll(firstIndex: LogIndex, data: Array[LogEntry[A]]): LogAppendResult = {
    latestAppendCache = None
    underlying.appendAll(firstIndex, data)
  }
  override def latestCommit(): LogIndex = {
    latestCommitCache.getOrElse {
      val value = underlying.latestCommit()
      latestCommitCache = Option(value)
      value
    }
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
}
