package riff.monix
import monix.execution.Scheduler
import monix.reactive.Observable
import monix.reactive.subjects.Var
import riff.raft.node.RoleCallback.RoleEvent
import riff.raft.node.RoleCallback

/**
  * Provides a means to represent a [[riff.raft.node.RaftNode]]'s role as an Observable:
  *
  * {{{
  *
  *   val node : RaftNode[A] = ...
  *   val obs = ObservableState()
  *   node.withRoleCallback(obs)
  *
  *   // get events of this node's idea of who the leader is and its role
  *   obs.subscribe(...)
  * }}}
  */
class ObservableState(implicit sched: Scheduler) extends RoleCallback {
  private val eventsVar: Var[RoleEvent] = Var[RoleEvent](null)
  override def onEvent(event: RoleEvent): Unit = {
    eventsVar := event
  }
  def events: Observable[RoleEvent] = eventsVar.filter(_ != null)
}

object ObservableState {

  def apply()(implicit sched: Scheduler): ObservableState = {
    new ObservableState()
  }
}
