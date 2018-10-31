package riff.raft.log
import riff.raft.{LogIndex, NodeId, Term}
import riff.raft.messages.AppendEntriesResponse

import scala.util.control.NoStackTrace

sealed trait LogAppendResult

object LogAppendResult {

  def apply(firstIndex: LogCoords, lastIndex: LogCoords, replacedIndices: Seq[LogIndex] = Nil) = {
    LogAppendSuccess(firstIndex, lastIndex, replacedIndices)
  }
}

/**
  * Represents the return type of a file-based raft log
  *
  * @param firstIndex the first index written
  * @param lastIndex the last index written
  * @param replacedIndices in the case where a disconnected leader had accepted commits, these are the indices of replaced entries from a new leader
  */
final case class LogAppendSuccess(firstIndex: LogCoords, lastIndex: LogCoords, replacedIndices: Seq[LogIndex] = Nil)
    extends LogAppendResult {
  require(firstIndex.term == lastIndex.term, s"appended result w/ ${firstIndex} to ${lastIndex}")
  def numIndices = lastIndex.index - firstIndex.index + 1

  def appendedCoords: Set[LogCoords] = {
    val term = firstIndex.term
    (firstIndex.index to lastIndex.index).map { logIndex =>
      LogCoords(term, logIndex)
    }.toSet
  }

  def contains(response: AppendEntriesResponse): Boolean = {
    response.term == firstIndex.term && (response.matchIndex >= firstIndex.index && response.matchIndex <= lastIndex.index)
  }
}

final case class AttemptToSkipLogEntry(attemptedLogEntry: LogCoords, expectedNextIndex: LogIndex)
    extends Exception(
      s"Attempt to skip a log entry by appending ${attemptedLogEntry.index} w/ term ${attemptedLogEntry.term} when the next expected entry should've been $expectedNextIndex")
    with LogAppendResult with NoStackTrace
//final case class AttemptToAppendEntryWithEarlierTerm(attemptedAppend :LogCoords, latestLogEntry : LogCoords) extends Exception(s"Attempt to append an entry ${attemptedAppend} which has a term greater that our latest log entry w/ $latestLogEntry")
final case class AttemptToAppendLogEntryAtEarlierTerm(attemptedEntry: LogCoords, latestLogEntryAppended: LogCoords)
    extends Exception(
      s"An attempt to append ${attemptedEntry.index} w/ term ${attemptedEntry.term} when our latest entry was $latestLogEntryAppended. If an election took place after we were the leader, the term should've been incremented")
    with LogAppendResult with NoStackTrace

final case class NotTheLeaderException(attemptedNodeId: NodeId, term: Term, leadIdOpt: Option[NodeId])
  extends Exception(
    s"Attempt to append to node '${attemptedNodeId}' in term ${term}${leadIdOpt.fold("")(name =>
      s". The leader is ${name}")}"
  ) with LogAppendResult with NoStackTrace
