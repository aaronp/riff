package riff.monix.log

import monix.execution.Scheduler
import monix.reactive.Observable
import monix.reactive.observables.ConnectableObservable
import monix.reactive.subjects.{PublishToOneSubject, Var}
import riff.monix.RiffSchedulers
import riff.raft.LogIndex
import riff.raft.log._

/**
  * Wraps a [[riff.raft.log.RaftLog]] for a local node which exposes Observable data publishers for:
  * $ Appended [[LogCoords]]
  * $ Appended [[LogEntry]]
  * $ Appended [[LogCoords]] from a historic index
  * $ Appended [[LogEntry]] from a historic index
  * $ Committed [[LogCoords]]
  * $ Committed [[LogEntry]]
  * $ Committed [[LogCoords]] from a historic index
  * $ Committed [[LogEntry]] from a historic index
  *
  * @param underlying the wrapped log
  * @param scheduler the scheduler to use for the observables
  * @tparam A
  */
case class ObservableLog[A](override val underlying: RaftLog[A])(implicit scheduler: Scheduler) extends DelegateLog[A] with CommittedOps[A] with AppendOps[A] {

  private val appendedVar = Var[LogAppendSuccess](null: LogAppendSuccess)
  private val committedVar = Var[LogCommitted](Nil)

  /** @param index the (one based!) index from which we'd like to read the committed coords
    * @return an observable of all committed entries from the given index
    */
  override def committedCoordsFrom(index: LogIndex): Observable[LogCoords] = {
    coordsFrom(index, committedCoords, latestCommittedIndex())
  }

  /** @return an observable of committed coordinates from the point of subscription
    */
  override def committedCoords(): Observable[LogCoords] =
    committedVar.filter(_.nonEmpty).flatMap(Observable.fromIterable)

  /** @return an observable of the appended BUT NOT YET committed entries
    */
  override def appendResults(): Observable[LogAppendSuccess] = appendedVar.filter(_ != null)

  /** @param index the (one based!) index from which we'd like to read the appended coords
    * @return an observable of all appended (not necessarily committed) entries from the given index
    */
  override def appendedCoordsFrom(index: LogIndex): Observable[LogCoords] =
    coordsFrom(index, appendCoords, latestAppendedIndex())

  override protected def dataForIndex(coords: LogCoords): Observable[(LogCoords, A)] = {
    entryForIndex(coords.index) match {
      case Some(entry) => Observable.pure(coords -> entry.data)
      case None => Observable.raiseError(new Exception(s"Couldn't read an entry for $coords"))
    }
  }

  def latestCommittedIndex(): LogIndex = committedVar().lastOption.getOrElse(LogCoords.Empty).index

  def latestAppendedIndex(): LogIndex = Option(appendedVar()).map(_.lastIndex.index).getOrElse(1)

  private def coordsFrom(fromIndex: LogIndex, coords: Observable[LogCoords], readLatestReceivedIndex: => LogIndex): Observable[LogCoords] = {
    val subject = PublishToOneSubject[LogCoords]
    val connectable = ConnectableObservable.cacheUntilConnect(coords, subject)

    // our connectable observable *should* include all the data from the current value of counter, if not
    // before (as there seems to be race condition between setting up/connecting the observable and getting the historic elements)
    val indexFromWhichWeSwap: LogIndex = readLatestReceivedIndex

    // at this point we'll have all the data from 'counter'
    connectable.connect()

    val exactlyAfterIndex = connectable.filter(_.index >= fromIndex)

    if (indexFromWhichWeSwap >= fromIndex) {
      // we have some historic entries to provide
      val historicCoords = coordsBetween(fromIndex, indexFromWhichWeSwap)
      historicCoords ++ exactlyAfterIndex
    } else {
      exactlyAfterIndex
    }
  }

  private def coordsBetween(from: LogIndex, to: LogIndex): Observable[LogCoords] = {
    Observable.fromIterable(from.max(1) to to).flatMap { idx =>
      coordsForIndex(idx) match {
        case Some(d) => Observable.pure(d)
        case None => Observable.empty[LogCoords]
      }
    }
  }

  override def appendAll(fromIndex: LogIndex, data: Array[LogEntry[A]]): LogAppendResult = {
    val result: LogAppendResult = underlying.appendAll(fromIndex, data)

    result match {
      case ok: LogAppendSuccess => appendedVar := ok
      case _ =>
    }

    result
  }

  override def commit(index: LogIndex): Seq[LogCoords] = {
    val result: Seq[LogCoords] = underlying.commit(index)
    if (result.nonEmpty) {
      committedVar := result
    }
    result
  }
}

object ObservableLog {

  /**
    * Provide a convenience method to ensure we have an observable log:
    *
    * {{{
    *   import ObservableLog._
    *
    *   val log = RaftLog.inMemory[String].asObservable // or RaftLog.inMemory[String].cached.asObservable
    *
    *   // do something w/ the ObservableLog
    *   log.committedEntries.foreach(println)
    * }}}
    *
    * @param log the log which may or may not already be observable
    * @tparam A
    */
  implicit class AsRichLog[A](log: RaftLog[A]) {

    /** @return either this log if it is already observable, otherwise an [[ObservableLog]] wrapping the given node
      */
    def observable(implicit sched: Scheduler): ObservableLog[A] = {
      log match {
        case obs: ObservableLog[A] => obs
        case other => ObservableLog(other)
      }
    }

    /** @return the log as an observable log if it is one, or
      */
    def asObservable: Option[ObservableLog[A]] = {
      log match {
        case obs: ObservableLog[A] => Option(obs)
        case log: DelegateLog[A] => log.underlying.asObservable
        case _ => None
      }
    }
  }
}
