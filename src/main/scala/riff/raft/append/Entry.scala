package riff.raft.append

final case class Entry[T](term: Int, data: T)
