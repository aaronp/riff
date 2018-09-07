package riff.raft

import riff.raft.log.LogCoords

//final case class RedirectException(leaderOpinion: LeaderOpinion) extends Exception(s"${leaderOpinion}")

final case class LogAppendException[T](coords: LogCoords, data: T, err: Throwable) extends Exception(s"Error appending ${coords} : $data", err)
final case class LogCommitException(coords: LogCoords, err: Throwable)             extends Exception(s"Error committing ${coords}", err)

trait LogAppendError extends Exception
final case class AttemptToCommitMissingIndex(attemptedIndex : LogIndex) extends Exception(s"couldn't find the term for $attemptedIndex")
final case class AttemptToOverwriteACommittedIndex(attemptedLogIndex : LogIndex, latestCommittedIndex : LogIndex) extends Exception(s"Attempt to append $attemptedLogIndex when the latest committed index is $latestCommittedIndex")
