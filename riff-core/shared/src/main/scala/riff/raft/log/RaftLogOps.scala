package riff.raft.log

import riff.raft.messages.{AppendEntries, AppendEntriesResponse}
import riff.raft.{LogIndex, Term}

import scala.reflect.ClassTag

/**
  * Additional convenience operations which can be performed on a RaftLog
  *
  * @tparam A
  */
trait RaftLogOps[A] { self: RaftLog[A] =>

  /** Append all the data entries
    *
    * @param coords
    * @param data
    * @return
    */
  def appendAll(fromIndex: LogIndex, data: Array[LogEntry[A]]): LogAppendResult

  /**
    * Append the given log entry w/ the given coords (term and index)
    *
    * @param coords the log term and index against which this log should be appended
    * @param data
    * @return the append result
    */
  def append(coords: LogCoords, data: A, theRest: A*)(implicit classTag: ClassTag[A]): LogAppendResult = {
    val firstEntry = LogEntry(coords.term, data)
    val arr = if (theRest.isEmpty) {
      Array(firstEntry)
    } else {
      firstEntry +: theRest.map(v => LogEntry(coords.term, v)).toArray
    }
    appendAll(coords.index, arr)
  }

  def latestCommit(): LogIndex

  /** @param index
    * @return the log term for the latest index
    */
  def termForIndex(index: LogIndex): Option[Term]

  def latestAppended(): LogCoords

  /** @param entry the entry to check
    * @return true if there is a log entry for the given coords
    */
  def contains(entry: LogCoords): Boolean = termForIndex(entry.index).exists(_ == entry.term)

  /**
    * Commit all the entries up to the given index.
    *
    * It is the responsibility of the node to determine whether this should be called, knowing
    * that this log is safe to commit
    *
    * @param index the index to commit
    * @return the coordinates of all entries committed
    */
  def commit(index: LogIndex): Seq[LogCoords]

  def entryForIndex(index: LogIndex): Option[LogEntry[A]]

  /**
    * @return the latest appended entry LogState (term and index)
    */
  def logState(): LogState = {
    val LogCoords(term, index) = latestAppended()
    LogState(latestCommit(), term, index)
  }

  final def coordsForIndex(index: LogIndex): Option[LogCoords] = termForIndex(index).map(LogCoords(_, index))

  /** @return a caching version of this log
    */
  def cached(): CachingLog[A] = {
    this match {
      case c: CachingLog[A] => c
      case other            => new CachingLog[A](other)
    }
  }

  /**
    * @param firstIndex the matching log index, WHICH IS ONE-BASED
    * @param max the maximum entries to return
    * @return the log entries from the given match index
    */
  def entriesFrom(firstIndex: LogIndex, max: Int = Int.MaxValue): Array[LogEntry[A]] = {
    val mostRecentAppend = latestAppended()
    val opts             = (firstIndex.max(1) to mostRecentAppend.index).view.map(entryForIndex)
    opts
      .takeWhile(_.isDefined)
      .take(max)
      .collect {
        case Some(x) => x
      }
      .toArray
  }

  protected def containsIndex(index: LogCoords) = {
    entryForIndex(index.index).exists(_.term == index.term)
  }

  /**
    * append the entry to the log.
    *
    * If the log entry matches _any_ of this raft node's log entries then this is successful, and the match
    * index will be set to the request previous index + 1.
    *
    * @param raftNode
    * @param request
    * @return the AppendEntriesResponse
    */
  def onAppend(currentTerm: Term, request: AppendEntries[A]): AppendEntriesResponse = {
    val success = {
      //
      // we're successful if
      // 1) common case - it's another append on top of our latest index
      // 2) we're a follower whose fallen behind. If that's the case, we need to
      //    a) check the previous matches our log
      //    -- or --
      //    b) the previous is the first index
      //
        val latest = latestAppended()
      val matchedPrevious = {
        latest == request.previous || containsIndex(request.previous)
      }

      // we could be in the situation where a leader appended an unreplicated entry and subsequently became a follower
      matchedPrevious || (request.previous.index == 0)
    }

    val matchIndex = if (success && request.entries.nonEmpty) {
      val latestCommitIndex = latestCommit()
      require(latestCommitIndex <= request.previous.index, s"Attempt to append at ${request.previous} when the latest commit is $latestCommitIndex")
      val logAppendResult: LogAppendResult = appendAll(request.appendIndex, request.entries)

      logAppendResult match {
        case LogAppendSuccess(_, lastIndex, _) => lastIndex
        case _                                 => 0
      }
    } else {
      0
    }

    // double-check - should we fail w/ the node's term or the latest log entry term?
    if (success) {
      AppendEntriesResponse.ok(request.term, matchIndex)
    } else {
      AppendEntriesResponse.fail(currentTerm)
    }
  }

}
