package riff.monix.log
import monix.reactive.Observable
import riff.raft.LogIndex
import riff.raft.log.{LogAppendResult, LogAppendSuccess, LogCoords}

trait AppendOps[A] {

  /** @return an observable of the appended BUT NOT YET committed entries
    */
  def appendResults(): Observable[LogAppendResult]

  /** @param index the (one based!) index from which we'd like to read the appended coords
    * @return an observable of all appended (not necessarily committed) entries from the given index
    */
  def appendedCoordsFrom(index: LogIndex): Observable[LogCoords]

  /** @return an observable of the appended BUT NOT YET committed entries from the time of subscription
    */
  def appendCoords(): Observable[LogCoords] = {
    appendResults.flatMap {
      case res: LogAppendSuccess =>
        if (res.firstIndex.index == res.lastIndex.index) {
          Observable.pure(res.firstIndex)
        } else {
          val coords = (res.firstIndex.index to res.lastIndex.index).map { idx =>
            res.firstIndex.copy(index = idx)
          }
          Observable.fromIterable(coords)
        }
      case err: Throwable => Observable.raiseError(err)
    }
  }

  /** @param index
    * @return an observable of log entries from the given index
    */
  def appendedEntriesFrom(index: LogIndex): Observable[(LogCoords, A)] = appendedCoordsFrom(index).flatMap(dataForIndex)

  /** @return an observable of the appended coordinates and data from the time of subscription
    */
  def appendedEntries(): Observable[(LogCoords, A)] = {
    // there's actually a race condition here between being notified of new coords and the 'dataForIndex'.
    // if an entry is replace in between the notification and the lookup, we could get fed e.g. one coord of:
    // LogCoords(term=2, index=2)
    appendCoords().dump("append coords").flatMap(dataForIndex).dump("dataForIndex")
  }

  protected def dataForIndex(coords: LogCoords): Observable[(LogCoords, A)]

}
