package riff.raft.log
import com.typesafe.scalalogging.LazyLogging
import riff.raft.LogIndex

import scala.collection.immutable

private[log] abstract class BaseLog[T] extends RaftLog[T] with LazyLogging {

  /**
    * we can get in this state if we've been leader and accepted some append commands from a client,
    * only to then discover there was a leader election and we were voted out, in which case we may
    * have extra, invalid uncommitted entries
    *
    * @param coordsOfTheFirstNewEntryToAppend the latest append coords
    * @return the indices to remove
    */
  protected def checkForOverwrite(coordsOfTheFirstNewEntryToAppend: LogCoords): immutable.Seq[LogIndex] = {
    val latest = latestAppended()

    // if our latest index is the same or after the
    if (latest.index >= coordsOfTheFirstNewEntryToAppend.index) {

      require(
        coordsOfTheFirstNewEntryToAppend.term > latest.term,
        s"Attempt to append $coordsOfTheFirstNewEntryToAppend when our latest term is $latest. If an election took place after we were the leader, the term should've been incremented"
      )

      logger.warn(
        s"Received append for $coordsOfTheFirstNewEntryToAppend when our last entry was $latest. Assuming we're not the leader and clobbering invalid indices")
      (coordsOfTheFirstNewEntryToAppend.index to latest.index)
    } else {
      // the coords are after our term
      require(coordsOfTheFirstNewEntryToAppend.term >= latest.term, s"Attempt to append an entry w/ an earlier term that our latest entry $latest")
      require(
        coordsOfTheFirstNewEntryToAppend.index == latest.index + 1,
        s"Attempt to skip a log entry by appending $coordsOfTheFirstNewEntryToAppend when the latest entry was $latest"
      )
      Nil
    }
  }

  /**
    * hook for subclasses after having determined which entries need to be committed.
    *
    * @param index the index to commit
    * @param entriesToCommit the entries determined which need to be committed
    */
  protected def doCommit(index: LogIndex, entriesToCommit: immutable.IndexedSeq[LogCoords]): Unit

  override final def commit(index: LogIndex): Seq[LogCoords] = {
    val previous = latestCommit()
    if (previous < index) {
      val committed: immutable.IndexedSeq[LogCoords] = ((previous + 1) to index).map { i =>
        val term = termForIndex(i).getOrElse(sys.error(s"couldn't find the term for $i"))
        LogCoords(term, i)
      }

      doCommit(index, committed)

      committed
    } else {
      Nil
    }
  }

  protected def assertCommit(coords: LogCoords) = {
    val kermit = latestCommit
    require(kermit < coords.index, s"Attempt to append $coords when the latest committed index is $kermit")
    kermit
  }
}
