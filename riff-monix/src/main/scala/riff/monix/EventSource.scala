package riff.monix
import java.nio.file.Path

import eie.io.{FromBytes, ToBytes, _}
import monix.reactive.Observable
import riff.monix.log.CommittedOps
import riff.raft.LogIndex
import riff.raft.log.LogCoords

import scala.util.{Success, Try}

/**
  * Contains a function for applying a log type A to a state S, and persisting that state.
  *
  *
  * For example, the log entries may be Strings and the State 'S' could be Set[String].
  * Or perhaps the log entries are a sealed trait of CRUD operations, and the State is the database.
  *
  * If the State itself is too big to fit in memory, then it could just be a cache over top a persistent storage.
  *
  * If you don't want to persist the state and just e.g. event source from the beginning of the log, then this all becomes
  * a lot easier (just a one-liner, folding A over the state S).
  */
object EventSource {

  /**
    * Provides a means to track snapshots for the log.
    * Instead of observing the log directly, the EventSource observes the log and ever N updates produces a snapshot.
    *
    * This is typically more useful than observing the logs directly, as it provides a way to represent your application's
    * state machine (of type 'S') by applying single log entries to it (of type 'A').
    *
    * Observers of the snapshot will get the latest snapshot version and the raw log entries ;-)
    *
    * This is an alternative to log compaction.
    *
    * @param dataDir the directory under which snapshots should be written
    * @param initial the initial state should none be available
    * @param log a means of obtaining the 'committedEntriesFrom'
    * @param snapEvery the frequency of how often we should write down the snapshots
    * @param combine a function to combine the log entries of type 'A' w/ the state
    * @tparam S the state type
    * @tparam A the log entry type
    * @tparam S the snapshot type
    * @tparam A the log entry type
    */
  def apply[S: FromBytes: ToBytes, A](
    dataDir: Path,
    initial: => S,
    log: CommittedOps[A],
    snapEvery: Int,
    numberToKeep: Option[Int] = None)(combine: (S, A) => S): Try[Observable[S]] = {
    apply(dao(dataDir, initial, numberToKeep), log, snapEvery)(combine)
  }

  def dao[S: FromBytes: ToBytes, A](dataDir: Path, initial: => S, numberToKeep: Option[Int] = None) = {
    new FileDao(dataDir, initial, numberToKeep)
  }

  /** @param dao the access object for obtaining the snaphot
    * @param log a means of obtaining the 'committedEntriesFrom'
    * @param snapEvery the frequency of how often we should write down the snapshots
    * @param combine a function to combine the log entries of type 'A' w/ the state
    * @tparam S the state type
    * @tparam A the log entry type
    * @return an observable of the updated state
    */
  def apply[S, A](dao: StateDao[S], log: CommittedOps[A], snapEvery: Int)(combine: (S, A) => S): Try[Observable[S]] = {
    dao.latestSnapshot().map {
      case (latestIndex, latestSnapshot) =>
        val entries: Observable[(LogCoords, A)] =
          log.committedEntriesFrom(latestIndex)
        entries.zipWithIndex
          .scan(latestSnapshot) {
            case (state, ((coords, next), i)) =>
              val newState = combine(state, next)
              if (i != 0 && i % snapEvery == 0) {
                dao.writeDown(coords, newState)
              }
              newState
          }
    }
  }

  /**
    * Represents the IO needed to persist and retrieve a snapshot
    *
    * @tparam S the snapshot type
    */
  trait StateDao[S] {

    /** means to get the latest snapshot index and state.
      * If no snapshot is available, this function should return 0 and some empty value for S
      *
      * @return the most recently written snapshot index and snapshot
      */
    def latestSnapshot(): Try[(LogIndex, S)]

    /**
      * saves the snapshot together w/ the most recent log entry (coords) which were applied to it
      *
      * @param coords the most recent log coordinates which were applied to the state S
      * @param s the snapshot type
      */
    def writeDown(coords: LogCoords, s: S): Unit
  }

  /**
    *
    * A file-based StateDao implementation which writes a '.latestSnapshotIndex' file under the given directory
    * to track what the last snapshot taken was
    *
    * It then writes down snapshot-X files to contain the persisted snapshots
    *
    * @param dataDir the directory under which files will be written
    * @param empty the default empty state
    * @param numberToKeep if specified, snapshots older than this value will be removed at the point the new snapshots are written
    * @param ev$1
    * @param ev$2
    * @tparam S the snapshot type
    */
  class FileDao[S: FromBytes: ToBytes](dataDir: Path, empty: => S, numberToKeep: Option[Int]) extends StateDao[S] {
    private val latestSnapshotIndexFile: Path = dataDir.resolve(".latestSnapshotIndex")
    if (!latestSnapshotIndexFile.exists()) {
      updateLatestSnapshotCoords(LogCoords.Empty)
    }

    override def latestSnapshot(): Try[(LogIndex, S)] = {
      val index = latestSnapshotIndexFile.text match {
        case LogCoords.FromKey(coords) => coords.index
        case _ => 0
      }

      if (index == 0) {
        Success(0 -> empty)
      } else {
        readSnapshot(index).map(index -> _)
      }
    }

    private def readSnapshot(index: Int): Try[S] = {
      FromBytes[S].read(snapshotFileForIndex(index).bytes)
    }

    override def writeDown(coords: LogCoords, s: S) = {
      import ToBytes.ops._
      latestSnapshotIndexFile.text = coords.asKey
      snapshotFileForIndex(coords.index).bytes = s.bytes
      numberToKeep.foreach { offset =>
        val deleteMe = snapshotFileForIndex(coords.index - offset)
        if (deleteMe.exists()) {
          deleteMe.delete()
        }
      }
    }

    private def updateLatestSnapshotCoords(coords: LogCoords) = {
      latestSnapshotIndexFile.text = coords.asKey
    }
    private def snapshotFileForIndex(idx: LogIndex): Path = dataDir.resolve(s"snapshot-${idx}")
  }

}
