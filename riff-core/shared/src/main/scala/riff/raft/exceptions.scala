package riff.raft

import riff.raft.log.LogCoords

final case class LogAppendException[T](coords: LogCoords, data: T, err: Throwable)
    extends Exception(s"Error appending ${coords} : $data", err)
final case class LogCommitException(coords: LogCoords, err: Throwable)
    extends Exception(s"Error committing ${coords}", err)

trait LogAppendError extends Exception
final case class AttemptToCommitMissingIndex(attemptedIndex: LogIndex)
    extends Exception(s"couldn't find the term for $attemptedIndex")
final case class AttemptToOverwriteACommittedIndex(attemptedLogIndex: LogIndex, latestCommittedIndex: LogIndex)
    extends Exception(s"Attempt to append $attemptedLogIndex when the latest committed index is $latestCommittedIndex")
final case class NotTheLeaderException(attemptedNodeId: NodeId, term: Term, leadIdOpt: Option[NodeId])
    extends Exception(
      s"Attempt to append to node '${attemptedNodeId}' in term ${term}${leadIdOpt.fold("")(name =>
        s". The leader is ${name}")}"
    )
