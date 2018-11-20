package riff.raft.log
import riff.raft._

import scala.collection.immutable

private[log] abstract class BaseLog[T] extends RaftLog[T] { // with LazyLogging {

  /**
    * we can get in this state if we've been leader and accepted some append commands from a client,
    * only to then discover there was a leader election and we were voted out, in which case we may
    * have extra, invalid uncommitted entries
    *
    * @param coordsOfTheFirstNewEntryToAppend the latest append coords
    * @return the indices to remove
    */
  protected def checkForOverwrite(firstIndex : LogIndex, firstTerm: Term): Either[LogAppendResult, immutable.Seq[LogCoords]] = {
    val latest: LogCoords = latestAppended()

    // if our latest index is the same or after the index to append, that implies
    // we were the leader who accepted an append while another node who didn't have that entry
    // won an election, and is now appending
    if (latest.index >= firstIndex) {

      // ... and if that's the case, then the term of that log entry should be > than the term for our entry
      if (firstTerm <= latest.term) {
        Left(AttemptToAppendLogEntryAtEarlierTerm(LogCoords(firstTerm, firstIndex), latest))
      } else {
        Right((firstIndex to latest.index).flatMap(coordsForIndex))
      }
    } else {
      // the coords are after our term
      if (firstTerm < latest.term) {
        Left(AttemptToAppendLogEntryAtEarlierTerm(LogCoords(firstTerm, firstIndex), latest))
      } else if (firstIndex != latest.index + 1) {
        Left(AttemptToSkipLogEntry(LogCoords(firstTerm, firstIndex), latest.index + 1))
      } else {
        Right(Nil)
      }
    }
  }

  /**
    * hook for subclasses after having determined which entries need to be committed.
    *
    * @param index the index to commit
    * @param entriesToCommit the entries determined which need to be committed
    */
  protected def doCommit(index: LogIndex, entriesToCommit: immutable.IndexedSeq[LogCoords]): Unit

  override final def commit(index: LogIndex): Seq[LogCoords] = {
    val previous = latestCommit()
    if (previous < index) {
      val committed: immutable.IndexedSeq[LogCoords] = ((previous + 1) to index).map { i =>
        val term = termForIndex(i).getOrElse(throw AttemptToCommitMissingIndex(i))
        LogCoords(term, i)
      }

      doCommit(index, committed)

      committed
    } else {
      Nil
    }
  }

  protected def assertCommit(logIndex: LogIndex) = {
    val kermit = latestCommit
    if (kermit >= logIndex) {
      throw AttemptToOverwriteACommittedIndex(logIndex, kermit)
    }
    kermit
  }
}
