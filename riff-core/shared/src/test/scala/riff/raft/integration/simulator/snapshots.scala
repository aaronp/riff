package riff.raft.integration.simulator

import riff.raft.{NodeId, Term}
import riff.raft.log.{LogEntry, RaftLog}
import riff.raft.node._

case class NodeSnapshot[A](name: String,
                           role: NodeRole,
                           cluster: ClusterSnapshot,
                           persistentStateSnapshot: PersistentStateSnapshot,
                           leaderSnapshot: Option[LeaderSnapshot],
                           log: LogSnapshot[A]) {
  def pretty() = {
    val logStr = log.pretty("    ") match {
      case "" => ""
      case str    => s"$str"
    }

    s"""$name ($role) in ${cluster.pretty}
       |    $persistentStateSnapshot
       |${leaderSnapshot.fold("")(_.pretty)}${logStr}""".stripMargin
  }
}

object NodeSnapshot {
  def apply[A](node: RaftNode[A]): NodeSnapshot[A] = {
    new NodeSnapshot[A](
      node.nodeId,
      node.state().role,
      ClusterSnapshot(node.cluster),
      PersistentStateSnapshot(node.persistentState),
      node.state().asLeader.map(LeaderSnapshot.apply),
      LogSnapshot(node.log)
    )
  }
}

case class ClusterSnapshot(peers: Set[NodeId]) {
  def pretty = s"cluster of ${peers.size} peers: [${peers.toList.map(_.toString).sorted.mkString(",")}]"
}
object ClusterSnapshot {
  def apply[A](cluster: RaftCluster): ClusterSnapshot = {
    val peers = cluster.peers
    new ClusterSnapshot(peers.toSet.ensuring(_.size == peers.size))
  }
}

case class LogSnapshot[A](entries: List[LogEntry[A]], latestCommit: Int) {
  def pretty(indent: String = ""): String = {
    if (entries.isEmpty) {
      ""
    } else {
      entries.zipWithIndex
        .map {
          case (LogEntry(t, value), i) => s"${i.toString.padTo(3, ' ')} | ${t.toString.padTo(3, ' ')} | $value"
        }
        .mkString(s"${indent}latestCommit=$latestCommit\n$indent", s"\n$indent", s"")
    }
  }
}
object LogSnapshot {
  def apply[A](log: RaftLog[A]): LogSnapshot[A] = {
    new LogSnapshot(log.entriesFrom(0).toList, log.latestCommit())
  }
}

case class PersistentStateSnapshot(currentTerm: Term, votedForInTerm: Option[String]) {
  override def toString = s"term ${currentTerm}, voted for ${votedForInTerm.getOrElse("nobody")}"
}
object PersistentStateSnapshot {
  def apply(state: PersistentState): PersistentStateSnapshot = {
    val castle = state.currentTerm // current turm
    PersistentStateSnapshot(castle, state.votedFor(castle))
  }
}

case class LeaderSnapshot(view: Map[String, Peer]) {
  def pretty() = {
    if (view.isEmpty) {
      ""
    } else {
      val width = view.keySet.map(_.length).max
      view
        .map {
          case (name, p) => s"    ${name.padTo(width, ' ')} --> $p"
        }
        .mkString("", "\n", "")
    }
  }
}
object LeaderSnapshot {
  def apply(leader: LeaderNodeState): LeaderSnapshot = {
    new LeaderSnapshot(leader.clusterView.toMap)
  }
}
