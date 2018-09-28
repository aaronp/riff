package riff.monix

import monix.execution.Scheduler
import monix.reactive.Observable
import riff.raft.NodeId
import riff.raft.messages.RequestOrResponse
import riff.raft.node.RaftNode

case class MonixNode[A](underlying: RaftNode[A]) {}

object MonixNode {

  def lift[A](source: Observable[(NodeId, RequestOrResponse[A])], node: RaftNode[A])(implicit sched: Scheduler) = {

    source.map {
      case (from, input) =>
        val resp: node.Result = node.onMessage(from, input)

        resp
    }
  }

}
