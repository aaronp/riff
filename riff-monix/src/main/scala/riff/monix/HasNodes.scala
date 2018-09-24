package riff.raft.log
import monix.reactive.Observable
import riff.monix.MonixNode
import riff.raft.RaftClient

trait HasNodes[A] {
  def nodes: Set[MonixNode[A]]

  def nodeFor(name: String): Option[MonixNode[A]] = nodes.find(_.nodeId == name)

  def clientFor(name: String): Option[RaftClient[Observable, A]] = nodeFor(name).map(_.client)

  def leader() = nodes.find(_.isLeader)

}
