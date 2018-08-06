package riff.raft

import riff.raft.log.LogCoords

//final case class RedirectException(leaderOpinion: LeaderOpinion) extends Exception(s"${leaderOpinion}")

final case class LogAppendException[T](coords: LogCoords, data: T, err: Throwable) extends Exception(s"Error appending ${coords} : $data", err)
final case class LogCommitException(coords: LogCoords, err: Throwable)             extends Exception(s"Error committing ${coords}", err)
