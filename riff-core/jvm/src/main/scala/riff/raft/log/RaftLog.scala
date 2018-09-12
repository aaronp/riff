package riff.raft.log
import java.nio.file.Path

import eie.io.{FromBytes, ToBytes}

/**
  * Represents a persistent log
  *
  * @tparam T
  */
trait RaftLog[A] extends RaftLogOps[A]

object RaftLog {

  def apply[A: ToBytes: FromBytes](path: Path, createIfNotExists: Boolean = false): FileBasedLog[A] = {
    FileBasedLog[A](path, createIfNotExists)
  }

  def inMemory[A]() = new InMemory[A]

}
