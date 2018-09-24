package riff.monix.log
import monix.reactive.Observable
import riff.raft.LogIndex
import riff.raft.log.LogCoords

/**
  * Represents observable operations which can be performed on a [[riff.raft.log.RaftLog]] for committed log entries
  *
  * @tparam A the log entry type
  */
trait CommittedOps[A] {

  /** @return an observable of committed coordinates from the point of subscription
    */
  def committedCoords(): Observable[LogCoords]

  /** @param index the (one based!) index from which to observe
    * @return an observable of the log entries from a particular index
    */
  def committedEntriesFrom(index: LogIndex): Observable[(LogCoords, A)] =
    committedCoordsFrom(index).flatMap(dataForIndex)

  /** @return an Observable of the committed coords and their values from the moment of subscription
    */
  def committedEntries(): Observable[(LogCoords, A)] = committedCoords.flatMap(dataForIndex)

  /** @param index the (one based!) index from which we'd like to read the committed coords
    * @return an observable of all committed entries from the given index
    */
  def committedCoordsFrom(index: LogIndex): Observable[LogCoords]

  protected def dataForIndex(coords: LogCoords): Observable[(LogCoords, A)]
}
