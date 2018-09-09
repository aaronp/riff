package riff.raft.integration

import riff.raft.Term
import riff.raft.log.{LogEntry, RaftLog}
import riff.raft.node._

case class NodeSnapshot[A](name: String,
                           role: NodeRole,
                           persistentStateSnapshot: PersistentStateSnapshot,
                           leaderSnapshot: Option[LeaderSnapshot],
                           log: LogSnapshot[A]) {
  def pretty() = {
    s"""$name ($role)
       |$persistentStateSnapshot
       |${leaderSnapshot.fold("")(_.pretty)}${log.pretty}
     """.stripMargin
  }
}

object NodeSnapshot {
  def apply[A](node: NodeState[String, A]): NodeSnapshot[A] = {
    new NodeSnapshot[A](
      node.nodeKey,
      node.raftNode().role,
      PersistentStateSnapshot(node.persistentState),
      node.raftNode().asLeader.map(LeaderSnapshot.apply),
      LogSnapshot(node.log)
    )
  }
}

case class LogSnapshot[A](entries: List[LogEntry[A]]) {
  def pretty(): String = {
    entries.zipWithIndex
      .map {
        case (LogEntry(t, value), i) => s"${i.toString.padTo(3, ' ')} | ${t.toString.padTo(3, ' ')} | $value"
      }
      .mkString("\n")
  }
}
object LogSnapshot {
  def apply[A](log: RaftLog[A]): LogSnapshot[A] = {
    new LogSnapshot(log.entriesFrom(0).toList)
  }
}

case class PersistentStateSnapshot(currentTerm: Term, votedForInTerm: Option[String])
object PersistentStateSnapshot {
  def apply(state: PersistentState[String]): PersistentStateSnapshot = {
    val castle = state.currentTerm // current turm
    PersistentStateSnapshot(castle, state.votedFor(castle))
  }
}

case class LeaderSnapshot(view: Map[String, Peer]) {
  def pretty() = {
    if (view.isEmpty) ""
    else {

      val width = view.keySet.map(_.length).max
      view
        .map {
          case (name, p) => s"    ${name.padTo(width, ' ')} --> $p"
        }
        .mkString("\n")
    }
  }
}
object LeaderSnapshot {
  def apply(leader: LeaderNode[String]) = {
    new LeaderSnapshot(leader.clusterView.toMap)
  }
}
