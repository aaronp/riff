package riff.monix

import monix.execution.Ack.{Continue, Stop}
import monix.execution.{Ack, Scheduler}
import monix.reactive.{Observer, Pipe}
import riff.raft.NodeId
import riff.raft.messages.RequestOrResponse
import riff.raft.node.NoOpResult.LogMessageResult
import riff.raft.node.{AddressedRequest, AddressedResponse, RaftCluster, RaftNode}

import scala.concurrent.Future

class MonixNode[A] private (val underlying: RaftNode[A])(implicit sched: Scheduler) {

  val id: NodeId = underlying.nodeKey

  private val (feed, src) = Pipe.publish[(NodeId, RequestOrResponse[A])].multicast
  def channel = feed
  def source = src
  private def handle(elem: (NodeId, RequestOrResponse[A]), byId: Map[NodeId, MonixNode[A]]): Future[Ack] = {
    val (from, msg) = elem
    val res: underlying.Result = underlying.onMessage(from, msg)

    res match {
      case AddressedRequest(requests) =>
        val ok = requests.forall {
          case (to, result) =>
            byId(to).channel.onNext(id, result) == Continue
        }
        if (ok) {
          Continue
        } else {
          Stop
        }
      case AddressedResponse(backTo, response) =>
        byId(backTo).channel.onNext(id, response)
      case LogMessageResult(msg) =>
        println(s"$id : $msg")
        Continue
    }
  }

  def subscribeToCluster(byId: Map[NodeId, MonixNode[A]]) = {
    val obs = new Observer[(NodeId, RequestOrResponse[A])] {
      override def onNext(elem: (NodeId, RequestOrResponse[A])) : Future[Ack] = {
        handle(elem, byId)
      }
      override def onError(ex: Throwable): Unit = {
        println(s"$id is $ex")
      }
      override def onComplete(): Unit = {
        println(s"$id is done")
      }
    }

    val peers = byId - id
    peers.values.foreach(_.source.subscribe(obs))
  }

}

object MonixNode {

  def of[A](nrNodes: Int)(newNode: String => RaftNode[A])(implicit s: Scheduler) = {
    val nodesNames = (1 to nrNodes).map { id => s"node $id"
    }.toSet

    val nodes: List[MonixNode[A]] = nodesNames.toList.map { name =>
      val cluster = RaftCluster(nodesNames - name)
      val node: RaftNode[A] = newNode(name).withCluster(cluster)
      new MonixNode[A](node)
    }

    val byId: Map[NodeId, MonixNode[A]] = nodes.groupBy(_.id).mapValues(_.head)
    byId.values.foreach(_.subscribeToCluster(byId))

    byId
  }
}
