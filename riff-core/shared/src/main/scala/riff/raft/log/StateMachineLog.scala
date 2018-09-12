package riff.raft.log
import riff.raft.LogIndex

/**
  * Invokes a function when log entries are committed
  *
  * @param underlying the wrapped RaftLog
  * @param onCommitted the side-effectful state-machine function
  * @tparam A
  */
class StateMachineLog[A](override val underlying: RaftLog[A], onCommitted: LogEntry[A] => Unit) extends DelegateLog[A] {

  override def commit(index: LogIndex): Seq[LogCoords] = {
    val committed = super.commit(index)

    if (committed.nonEmpty) {
      committed.foreach {
        case LogCoords(_, index) =>
          entryForIndex(index).foreach(onCommitted)
      }
    }

    committed
  }
}

object StateMachineLog {
  def apply[A](underlying: RaftLog[A])(applyToStateMachine: LogEntry[A] => Unit): StateMachineLog[A] = new StateMachineLog[A](underlying, applyToStateMachine)
}
