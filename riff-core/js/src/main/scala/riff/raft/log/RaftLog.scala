package riff.raft.log

import riff.raft._
import org.scalajs.dom.window

import scala.collection.immutable

/**
  * Represents a persistent log
  *
  * @tparam T
  */
trait RaftLog[A] extends RaftLogOps[A]

object RaftLog {

  def local[A: StringFormat]() = apply[A](Repo(window.localStorage))

  def session[A: StringFormat]() = apply[A](Repo[A](window.sessionStorage))

  def apply[A: StringFormat](storage: Repo): RaftLog.JSLog[A] = new JSLog[A](storage)

  class JSLog[A: StringFormat](repo: Repo) extends BaseLog[A] {
    private object Keys {
      val LatestCommitted                = ".latestCommitted"
      val LatestAppended                 = ".latestAppended"
      def entryForIndex(index: LogIndex) = s"${index}.entry"
      def termForIndex(index: LogIndex)  = s"$index.term"
    }
    override protected def doCommit(index: LogIndex, entriesToCommit: immutable.IndexedSeq[LogCoords]): Unit = {
      repo.setItem(Keys.LatestCommitted, index.toString)
    }

    override def appendAll(logIndex: LogIndex, data: Array[LogEntry[A]]): LogAppendResult = {
      if (data.isEmpty) {
        LogAppendResult(LogCoords.Empty, LogCoords.Empty)
      } else {
        doAppendAll(logIndex, data.head.term, data)
      }
    }

    private def doAppendAll(logIndex: LogIndex, firstTerm: Term, data: Array[LogEntry[A]]): LogAppendResult = {
      // sanity/consistency check that we're not clobbering over committed entries
      assertCommit(logIndex)

      // if another leader was elected while we were accepting append requests from some client, then our log may be wrong
      // that is to say, if we thought we were the leader and happily appended to our log, all the while having
      // been disconnected from the rest of the cluster (which may have gone on to elect a new leader while we were
      // disconnected), then we may have extra entries which we need to clobber
      checkForOverwrite(logIndex, firstTerm) match {
        case Left(err) => err
        case Right(indices) =>
          val removedIndices = indices.map { coords =>
            repo.removeItem(Keys.entryForIndex(coords.index))
            repo.removeItem(Keys.termForIndex(coords.index))
            coords
          }

          // write the log entries
          val appended: Array[LogCoords] = data.zipWithIndex.map {
            case (LogEntry(term, value), i) =>
              val index = logIndex + i

              repo.setItem(Keys.entryForIndex(index), StringFormat[A].asString(value: A))
              repo.setItem(Keys.termForIndex(index), term.toString)

              LogCoords(term, index)
          }

          // update latest appended
          val latestCoord = appended.last
          repo.setItem(Keys.LatestAppended, s"${latestCoord.term}:${latestCoord.index}")

          LogAppendResult(appended.head, latestCoord, removedIndices)
      }
    }

    override def latestCommit(): LogIndex = {
      repo.getItem(Keys.LatestCommitted) match {
        case None =>
          repo.setItem(Keys.LatestCommitted, 0.toString)
          latestCommit
        case Some(read) => read.toInt
      }
    }

    override def termForIndex(index: LogIndex): Option[Term] = {
      repo.getItem(Keys.termForIndex(index)).map(_.toInt)
    }
    override def latestAppended(): LogCoords = {
      val parsed = repo.getItem(Keys.LatestAppended).flatMap(LogCoords.FromKey.unapply)
      parsed.getOrElse(LogCoords.Empty)
    }
    override def entryForIndex(index: LogIndex): Option[LogEntry[A]] = {
      for {
        entryStr <- repo.getItem(Keys.entryForIndex(index))
        entry    <- StringFormat[A].fromString(entryStr).toOption
        termStr  <- repo.getItem(Keys.termForIndex(index))
      } yield {
        LogEntry[A](termStr.toInt, entry)
      }
    }
  }

  def inMemory[A]() = new InMemory[A]

}
