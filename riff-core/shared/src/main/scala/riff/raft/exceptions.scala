package riff.raft

import riff.raft.log.{LogAppendSuccess, LogCoords}

trait LogError extends Exception

final case class LogAppendException[T](coords: LogCoords, data: T, err: Throwable)
    extends Exception(s"Error appending ${coords} : $data", err) with LogError
final case class LogCommitException(coords: LogCoords, err: Throwable)
    extends Exception(s"Error committing ${coords}", err) with LogError
final case class AttemptToCommitMissingIndex(attemptedIndex: LogIndex)
    extends Exception(s"couldn't find the term for $attemptedIndex") with LogError
final case class AttemptToOverwriteACommittedIndex(attemptedLogIndex: LogIndex, latestCommittedIndex: LogIndex)
    extends Exception(s"Attempt to append $attemptedLogIndex when the latest committed index is $latestCommittedIndex") with LogError


trait ClientError extends Exception
final case class AppendOccurredOnDisconnectedLeader(originalAppend : LogAppendSuccess, newAppend : LogAppendSuccess) extends Exception(
  s"$originalAppend was appended but not committed by a disconnected leader and has later been replaced by $newAppend") with ClientError