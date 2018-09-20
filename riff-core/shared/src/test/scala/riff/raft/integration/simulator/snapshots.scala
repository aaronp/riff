package riff.raft.integration.simulator

import riff.raft.Term
import riff.raft.log.{LogEntry, RaftLog}
import riff.raft.node._

case class NodeSnapshot[A](name: String,
                           role: NodeRole,
                           cluster: ClusterSnapshot[String],
                           persistentStateSnapshot: PersistentStateSnapshot,
                           leaderSnapshot: Option[LeaderSnapshot],
                           log: LogSnapshot[A]) {
  def pretty() = {
    s"""$name ($role) in ${cluster.pretty}
       |    $persistentStateSnapshot
       |${leaderSnapshot.fold("")(_.pretty)}${log.pretty}
     """.stripMargin
  }
}

object NodeSnapshot {
  def apply[A](node: RaftNode[String, A]): NodeSnapshot[A] = {
    new NodeSnapshot[A](
      node.nodeKey,
      node.state().role,
      ClusterSnapshot(node.cluster),
      PersistentStateSnapshot(node.persistentState),
      node.state().asLeader.map(LeaderSnapshot.apply),
      LogSnapshot(node.log)
    )
  }
}

case class ClusterSnapshot[A](peers: Set[A]) {
  def pretty = s"cluster of ${peers.size} peers: [${peers.toList.map(_.toString).sorted.mkString(",")}]"
}
object ClusterSnapshot {
  def apply[A](cluster: RaftCluster[A]): ClusterSnapshot[A] = {
    val peers = cluster.peers
    new ClusterSnapshot(peers.toSet.ensuring(_.size == peers.size))
  }
}

case class LogSnapshot[A](entries: List[LogEntry[A]], latestCommit : Int) {
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
    new LogSnapshot(log.entriesFrom(0).toList, log.latestCommit())
  }
}

case class PersistentStateSnapshot(currentTerm: Term, votedForInTerm: Option[String]) {
  override def toString = s"term ${currentTerm}, voted for ${votedForInTerm.getOrElse("nobody")}"
}
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
  def apply(leader: LeaderNodeState[String]) = {
    new LeaderSnapshot(leader.clusterView.toMap)
  }
}
