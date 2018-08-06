package riff.raft.log

import java.nio.file.Path

import agora.io.ToBytes
import agora.io.implicits._
/**
  * Represents a persistent log
  *
  * @tparam T
  */
trait RaftLog[T] {
  type Result

  /**
    * Append the given log entry w/ the given coords (term and index)
    *
    * @param coords the log term and index against which this log should be appended
    * @param data
    * @return the append result
    */
  def append(coords: LogCoords, data: T): Result

  def latestCommit(): Int

  /** @param index
    * @return the log term for the latest index
    */
  def termForIndex(index: Int): Option[Int]

  def latestAppended(): LogCoords

  def logState: LogState = {
    val LogCoords(term, index) = latestAppended()
    LogState(latestCommit(), term, index)
  }

  /**
    * Commit all the entries up to the given index.
    *
    * It is the responsibility of the node to determine whether this should be called, knowing
    * that this log is safe to commit
    *
    * @param index
    */
  def commit(index: Int): Seq[LogCoords]
}

object RaftLog {

  def apply[T: ToBytes](dir: Path, createIfNotExists: Boolean = false): FileBasedLog[T] = {
    require(dir.isDir || (createIfNotExists && dir.mkDirs().isDir), s"$dir is not a directory")
    FileBasedLog[T](dir)
  }

  def inMemory[T](): RaftLog[T] = {
    new InMemory[T]
  }
}
