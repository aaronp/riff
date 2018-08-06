package riff.raft.log
import java.nio.file.Path
import java.nio.file.attribute.FileAttribute

import eie.io.{FromBytes, ToBytes}
import riff.raft.{LogIndex, Term}

import scala.collection.immutable

trait FileBasedLog[T] extends RaftLog[T] {
  def dir: Path
}

object FileBasedLog extends eie.io.LowPriorityIOImplicits {

  def apply[T: ToBytes: FromBytes](dir: Path, createIfNotExists: Boolean = false): FileBasedLog[T] = {
    require(dir.isDir || (createIfNotExists && dir.mkDirs().isDir), s"$dir is not a directory")
    new ForDir[T](dir)
  }

  val DefaultAttributes: Set[FileAttribute[_]] = {
    //    val perm = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("wrr"))
    //    Set(perm)
    Set.empty
  }

  /** This class is NOT thread safe.
    *
    * Saves entries in the form {{{
    * <dir>/<index>.entry
    * }}}
    * and
    * {{{
    *   <dir>/<index>.term
    *  }}}
    *
    * with the contents of <index>.entry being the bytes for the given value T
    *
    * When committed, the 0 byte file {{{<dir>/<index>_<term>.committed}}} will be created and the state file updated
    *
    * @param dir
    * @param ev$1
    * @tparam T
    */
  private class ForDir[T: ToBytes: FromBytes](override val dir: Path, fileAttributes: List[FileAttribute[_]] = DefaultAttributes.toList)
      extends BaseLog[T]
      with FileBasedLog[T] {

    private val commitFile = dir.resolve(".committed").createIfNotExists(fileAttributes: _*).ensuring(_.isFile)

    // contains the <term>:<index> of the latest entry appended
    private val latestAppendedFile = dir.resolve(".latestAppended").createIfNotExists(fileAttributes: _*).ensuring(_.isFile)
    private val LatestAppended     = """([0-9]+):([0-9]+)""".r

    override def entryForIndex(index: LogIndex) = {
      val path = entryFileForIndex(index)
      if (path.exists()) {
        FromBytes[T].read(path.bytes).toOption.map { value =>
          val term = termFileForIndex(index).text.toInt
          LogEntry(term, value)
        }
      } else {
        None
      }
    }

    override def appendAll(coords: LogCoords, data: Array[LogEntry[T]]): LogAppendResult = {

      // sanity/consistency check
      assertCommit(coords)

      // if another leader was elected while we were accepting append requests from some client, then our log may be wrong
      // that is to say, if we thought we were the leader and happily appended to our log, all the while having
      // been disconnected from the rest of the cluster (which may have gone on to elect a new leader while we were
      // disconnected), then we may have extra entries which we need to clobber
      val removedIndices = checkForOverwrite(coords).map { index =>
        termFileForIndex(index).deleteFile()
        entryFileForIndex(index).deleteFile()
        index
      }

      // write the log entries

      val appended: Array[LogIndex] = data.zipWithIndex.map {
        case (LogEntry(term, value), i) =>
          val index = coords.index + i
          // write the data
          entryFileForIndex(index).bytes = ToBytes[T].bytes(value)

          // write <index>.term to be the term
          termFileForIndex(index).text = term.toString
          index
      }

      // update the '.latestAppended' file w/ these coords as an optimisation so we don't have to search the file system
      // for that info
      val latestCoord = coords.copy(index = appended.last)
      updateLatestAppended(latestCoord)

      LogAppendResult(coords.term, appended.head, appended.last, removedIndices)
    }

    private def updateLatestAppended(coords: LogCoords) = {
      // update the persisted record of the latest appended
      latestAppendedFile.text = s"${coords.term}:${coords.index}"
    }

    private def entryFileForIndex(index: LogIndex) = dir.resolve(s"${index}.entry")
    private def termFileForIndex(index: LogIndex)  = dir.resolve(s"$index.term")

    override def termForIndex(index: LogIndex): Option[Term] = {
      Option(termFileForIndex(index)).filter(_.exists()).map(_.text.toInt)
    }

    override def latestCommit(): LogIndex = {
      commitFile.text match {
        case ""    => 0
        case value => value.toInt
      }
    }

    override def latestAppended(): LogCoords = {
      latestAppendedFile.text match {
        case LatestAppended(t, i) => LogCoords(term = t.toInt, index = i.toInt)
        case ""                   => LogCoords.Empty
        case other                => sys.error(s"Corrupt latest appended file ${latestAppendedFile} : >$other<")
      }
    }
    override protected def doCommit(index: LogIndex, entriesToCommit: immutable.IndexedSeq[LogCoords]): Unit = {
      commitFile.text = index.toString
      ()
    }
  }
}
