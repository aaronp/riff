package riff.monix
import monix.execution.Scheduler
import monix.reactive.Observable
import monix.reactive.subjects.Var
import riff.raft.RoleCallback
import riff.raft.RoleCallback.RoleEvent

/**
  * Provides a means to represent a [[riff.raft.node.RaftNode]]'s role as an Observable:
  *
  * {{{
  *
  *   val node : RaftNode[A] = ...
  *   val obs = NodeRoleObservable()
  *   node.withRoleCallback(obs)
  *
  *   // get events of this node's idea of who the leader is and its role
  *   obs.subscribe(...)
  * }}}
  */
class NodeRoleObservable(implicit sched: Scheduler) extends RoleCallback {
  private val events: Var[RoleEvent] = Var[RoleEvent](null)
  override def onEvent(event: RoleEvent): Unit = {
    events := event
  }
  def asObservable: Observable[RoleEvent] = events.filter(_ != null)
}

object NodeRoleObservable {

  def apply()(implicit sched: Scheduler): NodeRoleObservable = {
    new NodeRoleObservable()
  }
}
