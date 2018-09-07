package riff.raft.log
import java.nio.file.Path

import eie.io.{FromBytes, RichPath, ToBytes}

/**
  * Represents a persistent log
  *
  * @tparam T
  */
trait RaftLog[A] extends RaftLogOps[A]

object RaftLog {

  def apply[A: ToBytes: FromBytes](path: Path, createIfNotExists: Boolean = false): FileBasedLog[A] = {
    val dir = RichPath.asRichPath(path)
    require(dir.isDir || (createIfNotExists && RichPath.asRichPath(dir.mkDirs()).isDir), s"$path is not a directory")
    FileBasedLog[A](path)
  }

  def inMemory[A]() = new InMemory[A]

}
