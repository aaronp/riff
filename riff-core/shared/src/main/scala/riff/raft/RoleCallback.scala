package riff.raft
import riff.raft.RoleCallback.{NewLeaderEvent, RoleChangeEvent, RoleEvent}
import riff.raft.node.NodeRole

/**
  * This is provided as a convenience to be added to a [[riff.raft.node.RaftNode]] for anything which cares about
  * role transitions for a particular node.
  *
  * That behaviour could be done by subclassing the RaftNode, but subclassing brings in its own challenges/considerations.
  *
  * By making this more explicit, in addition to obviating the need to to subclass [[riff.raft.node.RaftNode]], it hopefully
  * will make the code more navigable/readable
  */
trait RoleCallback {

  /** the callback when a role transition takes place
    * @param event the event
    */
  def onEvent(event: RoleEvent): Unit

  /** signal there is a new leader. If the new leader is this node, BOTH a [[NewLeaderEvent]] and [[RoleChangeEvent]]
    * are triggered
    *
    * @param term the new term
    * @param leaderId the leader's nodeId
    */
  def onNewLeader(term: Term, leaderId: NodeId): Unit = {
    onEvent(NewLeaderEvent(term, leaderId))
  }

  def onRoleChange(term: Term, oldRole: NodeRole, newRole: NodeRole): Unit = {
    onEvent(RoleChangeEvent(term, oldRole, newRole))
  }
}

object RoleCallback {

  /**
    * The types of role events for the cluster
    */
  sealed trait RoleEvent

  /** Signalled when we get a heartbeat in a new term.
    * If the leader is the node sending this event, both a NewLeaderEvent and a [[RoleChangeEvent]] will be sent
    * @param term the current (new) term
    * @param leaderId the new leader id
    */
  case class NewLeaderEvent(term: Term, leaderId: NodeId) extends RoleEvent

  /** signalled whenever this node transitions to a new role
    *
    * @param term the new term
    * @param oldRole the previous role
    * @param newRole the new role
    */
  case class RoleChangeEvent(term: Term, oldRole: NodeRole, newRole: NodeRole) extends RoleEvent {
    require(oldRole != newRole)
  }

  object NoOp extends RoleCallback {
    override def onEvent(event: RoleEvent): Unit = {}
  }

  def apply(f : RoleEvent => Unit) = new RoleCallback {
    override def onEvent(event: RoleEvent): Unit = f(event)
  }
}
