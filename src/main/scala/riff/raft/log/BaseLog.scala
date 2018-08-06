package riff.raft.log
import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable

private[log] abstract class BaseLog[T] extends RaftLog[T] with LazyLogging {

  /**
    * we can get in this state if we've been leader and accepted some append commands from a client,
    * only to then discover there was a leader election and we were voted out, in which case we may
    * have extra, invalid uncommitted entries
    *
    * @param coords the latest append coords
    * @return the indices to remove
    */
  protected def checkForOverwrite(coords: LogCoords): immutable.Seq[Int] = {
    val latest = latestAppended()

    if (latest.index >= coords.index) {

      require(
        coords.term > latest.term,
        s"Attempt to append $coords when our latest term is $latest. If an election took place after we were the leader, the term should've been incremented"
      )

      logger.warn(s"Received append for $coords when our last entry was $latest. Assuming we're not the leader and clobbering invalid indices")
      (coords.index to latest.index)
    } else {
      // the coords are after our term
      require(coords.term >= latest.term, s"Attempt to append an entry w/ an earlier term that our latest entry $latest")
      require(coords.index == latest.index + 1, s"Attempt to skip a log entry by appending $coords when the latest entry was $latest")
      Nil
    }
  }

  protected def doCommit(index: Int): Unit

  override final def commit(index: Int): Seq[LogCoords] = {
    val previous = latestCommit()
    require(previous < index, s"asked to commit $index, but latest committed is $previous")

    val committed: immutable.IndexedSeq[LogCoords] = ((previous + 1) to index).map { i =>
      val term = termForIndex(i).getOrElse(sys.error(s"couldn't find the term for $i"))
      LogCoords(term, i)
    }

    doCommit(index)

    committed
  }
}
