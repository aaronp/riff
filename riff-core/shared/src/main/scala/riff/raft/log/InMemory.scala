package riff.raft.log
import riff.raft.{LogIndex, Term}

import scala.collection.immutable

/**
  * An in-memory persistent log. Not a great idea for production, but nifty for testing
  *
  * @tparam T
  */
class InMemory[T]() extends BaseLog[T] {
  private var mostRecentFirstEntries = List[(LogCoords, T)]()
  private var lastCommitted          = 0

  override protected def doCommit(index: LogIndex, entriesToCommit: immutable.IndexedSeq[LogCoords]): Unit = {
    require(lastCommitted < index)
    lastCommitted = index
  }

  override def appendAll(logIndex: LogIndex, data: Array[LogEntry[T]]): LogAppendResult = {
    require(logIndex > 0, s"log indices should begin at 1: $logIndex")
    if (data.isEmpty) {
      LogAppendResult(0, 0)
    } else {
      doAppendAll(logIndex, data.head.term, data)
    }
  }

  private def doAppendAll(logIndex: LogIndex, firstTerm : Term, data: Array[LogEntry[T]]): LogAppendResult = {
    assertCommit(logIndex)

    checkForOverwrite(logIndex, firstTerm) match {
      case Left(err) => err
      case Right(indicesToDelete) =>
        mostRecentFirstEntries = indicesToDelete.foldLeft(mostRecentFirstEntries) {
          case (list, i) => list.dropWhile(_._1.index >= i)
        }
        val newEntries: Array[(LogCoords, T)] = data.zipWithIndex.map {
          case (LogEntry(term, e), i) => LogCoords(term = term, index = logIndex + i) -> e
        }.reverse

        mostRecentFirstEntries match {
          case (head, _) :: _ =>
            require(logIndex == head.index + 1)
            mostRecentFirstEntries = newEntries.toList ++ mostRecentFirstEntries
          case tail =>
            require(logIndex == 1)
            mostRecentFirstEntries = newEntries.toList ++ tail
        }
        LogAppendResult(newEntries.last._1.index, newEntries.head._1.index, indicesToDelete)
    }
  }

  override def latestCommit(): LogIndex = {
    lastCommitted
  }

  override def entryForIndex(index: LogIndex) = {
    termForIndex(index).flatMap { term =>
      mostRecentFirstEntries.collectFirst {
        case (coords, value) if coords.term == term && index == coords.index => LogEntry(term, value)
      }
    }
  }
  override def termForIndex(index: LogIndex): Option[Term] = {
    mostRecentFirstEntries.collectFirst {
      case (LogCoords(term, `index`), _) => term
    }
  }
  override def latestAppended(): LogCoords = mostRecentFirstEntries.headOption.map(_._1).getOrElse(LogCoords.Empty)

  override def contains(entry: LogCoords): Boolean = mostRecentFirstEntries.exists(_._1 == entry)
}
