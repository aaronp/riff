package riff.monix.log

import monix.execution.Scheduler
import monix.reactive.Observable
import monix.reactive.subjects.Var
import riff.monix.RiffSchedulers
import riff.raft.log.{LogAppendResult, LogCoords, LogEntry, RaftLog}
import riff.raft.{LogIndex, Term}

/**
  * Wraps a [[riff.raft.log.RaftLog]] for a local node which can be observed
  *
  * @param underlying
  * @param scheduler
  * @tparam A
  */
case class ObservableLog[A](underlying: RaftLog[A])(implicit scheduler: Scheduler = RiffSchedulers.DefaultScheduler) extends RaftLog[A] {

  private val logView = Var[LogNotificationMessage](null: LogNotificationMessage)

  def asObservable: Observable[LogNotificationMessage] = logView.filter(_ != null)

  override def appendAll(coords: LogCoords, data: Array[LogEntry[A]]) = {
    val result: LogAppendResult = underlying.appendAll(coords, data)

    logView := LogAppended(result)

    result
  }

  override def latestCommit(): LogIndex = underlying.latestCommit()

  override def termForIndex(index: LogIndex): Option[Term] = underlying.termForIndex(index)

  override def latestAppended(): LogCoords = underlying.latestAppended()

  override def commit(index: LogIndex): Seq[LogCoords] = {
    val result = underlying.commit(index)

    logView := LogCommitted(result)

    result
  }
  override def entryForIndex(index: LogIndex) = underlying.entryForIndex(index)
}
