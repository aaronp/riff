package riff.raft.log
import java.nio.file.Path
import java.nio.file.attribute.FileAttribute

import agora.io.ToBytes
trait FileBasedLog[T] extends RaftLog[T] {
  type Result = LogAppendResult
  def dir: Path
}

object FileBasedLog {

  def apply[T: ToBytes](dir: Path, createIfNotExists: Boolean = false): FileBasedLog[T] = {
    import agora.io.implicits._
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
    * It also stores
    *
    * {{{<dir>/.state}}}
    *
    * @param dir
    * @param ev$1
    * @tparam T
    */
  private class ForDir[T: ToBytes](override val dir: Path, fileAttributes: List[FileAttribute[_]] = DefaultAttributes.toList)
      extends BaseLog[T]
      with FileBasedLog[T] {

    import agora.io.implicits._
    private val commitFile = dir.resolve(".committed").createIfNotExists(fileAttributes: _*).ensuring(_.isFile)

    // contains the <term>:<index> of the latest entry appended
    private val latestAppendedFile = dir.resolve(".latestAppended").createIfNotExists(fileAttributes: _*).ensuring(_.isFile)
    private val LatestAppended     = """([0-9]+):([0-9]+)""".r

    override def append(coords: LogCoords, data: T): Result = {

      // if another leader was elected while we were accepting appends, then our log may be wrong
      val entriesToRemove = checkForOverwrite(coords).map { index =>
        termFileForIndex(index).deleteFile()
        entryFileForIndex(index).deleteFile()
      }

      // update our file stating what our last commit was so we don't have to search the file system
      updateLatestAppended(coords)

      // update the persisted log
      writeTermEntryOnAppend(coords)

      // finally write our log entry
      val entryFile = entryFileForIndex(coords.index)
      entryFile.bytes = ToBytes[T].bytes(data)

      LogAppendResult(entryFile, entriesToRemove)
    }

    private def writeTermEntryOnAppend(coords: LogCoords) = {
      val kermit = latestCommit
      require(kermit < coords.index, s"Attempt to append $coords when the latest committed was $kermit")
      dir.resolve(s"${coords.index}.term").text = coords.term.toString
    }

    private def updateLatestAppended(coords: LogCoords) = {
      // update the persisted record of the latest appended
      latestAppendedFile.text = s"${coords.term}:${coords.index}"
    }

    private def entryFileForIndex(index: Int) = dir.resolve(s"${index}.entry")
    private def termFileForIndex(index: Int)  = dir.resolve(s"$index.term")

    override def termForIndex(index: Int): Option[Int] = {
      Option(termFileForIndex(index)).filter(_.exists()).map(_.text.toInt)
    }

    override def latestCommit(): Int = {
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
    override protected def doCommit(index: Int): Unit = {
      commitFile.text = index.toString
    }
  }
}
