package riff.raft.log

import java.nio.file.Path

import eie.io._
import riff.raft.messages.{AppendEntries, AppendEntriesResponse}
import riff.raft.{LogIndex, Term}

import scala.reflect.ClassTag

/**
  * Represents a persistent log
  *
  * @tparam T
  */
trait RaftLog[A] {

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
    val logMatches = latestAppended() == request.previous

    val success = logMatches && request.term == currentTerm

    val matchIndex = if (success && request.entries.nonEmpty) {
      val latestCommitIndex = latestCommit()
      require(latestCommitIndex <= request.previous.index, s"Attempt to append at ${request.previous} when the latest commit is $latestCommitIndex")
      val logAppendResult = appendAll(request.appendIndex, request.entries)
      logAppendResult.lastIndex
    } else {
      0
    }

    AppendEntriesResponse.ok(request.term, matchIndex)
  }

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
    appendAll(coords, arr)
  }

  def appendAll(coords: LogCoords, data: Array[LogEntry[A]]): LogAppendResult

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

  def logState: LogState = {
    val LogCoords(term, index) = latestAppended()
    LogState(latestCommit(), term, index)
  }

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
  final def coordsForIndex(index: LogIndex): Option[LogCoords] = termForIndex(index).map(LogCoords(_, index))

  def cached(): CachingLog[A] = {
    this match {
      case c: CachingLog[A] => c
      case other            => new CachingLog[A](other)
    }
  }
}

object RaftLog {

  def apply[A: ToBytes: FromBytes](path: Path, createIfNotExists: Boolean = false): FileBasedLog[A] = {
    val dir = RichPath.asRichPath(path)
    require(dir.isDir || (createIfNotExists && RichPath.asRichPath(dir.mkDirs()).isDir), s"$path is not a directory")
    FileBasedLog[A](path)
  }

  def inMemory[A]() = new InMemory[A]

}
