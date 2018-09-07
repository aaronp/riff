package riff.raft.log
import riff.raft.Term

final case class LogEntry[T](term: Term, data: T)
