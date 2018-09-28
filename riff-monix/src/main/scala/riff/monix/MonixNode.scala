package riff.monix

import monix.execution.Scheduler
import monix.reactive.{Observable, Observer, Pipe}
import riff.raft.NodeId
import riff.raft.messages.{RaftRequest, RequestOrResponse}
import riff.raft.node.NoOpResult.LogMessageResult
import riff.raft.node.{AddressedRequest, AddressedResponse, RaftCluster, RaftNode}

class MonixNode[A] private (
  val underlying: RaftNode[A],
  source: Observer[(NodeId, RequestOrResponse[A])],
  feed: Observable[(NodeId, RequestOrResponse[A])])(implicit sched: Scheduler) {

  val id: NodeId = underlying.nodeKey

  /**
    * subscribe this feed to ever other node, and ever other node to this feed
    */
  val channel: Observable[(NodeId, RequestOrResponse[A])] = feed.flatMap {
    case (destination, msg) if destination == id =>
      val res: underlying.Result = underlying.onMessage(destination, msg)
      res match {
        case AddressedRequest(requests) =>
          val obs: Observable[(NodeId, RaftRequest[A])] = Observable.fromIterable(requests)
          obs
        case AddressedResponse(backTo, response) =>
          Observable.pure(backTo -> response)
        case LogMessageResult(msg) =>
          println(msg)
          Observable.empty
      }
    case _ => Observable.empty
  }

//  override def onNext(elem: (NodeId, RequestOrResponse[A])): Future[Ack] = {
//    val (from, msg) = elem
//
//    val res: underlying.Result = underlying.onMessage(from, msg)
//
//    res match {
//      case AddressedRequest(requests) =>
//        val obs: Observable[(NodeId, RaftRequest[A])] = Observable.fromIterable(requests)
//
//
//
//        obs
//      case AddressedResponse(backTo, response) =>
//        Observable.pure(backTo -> response)
//      case LogMessageResult(msg) =>
//        println(msg)
//        Observable.empty
//    }
//
//    Continue
//  }

  def subscribeToCluster(byId: Map[NodeId, MonixNode[A]]) = {
    val peers: Map[NodeId, MonixNode[A]] = byId - id



    peers.map {
      case (from, peerNode) =>
      val toUs: Observable[(NodeId, RequestOrResponse[A])] = peerNode.feed.filter {
        case (toId, _) => toId == id
      }
      toUs.subscribe(source)
    }
    ???
  }

}

object MonixNode {

  def of[A](nrNodes: Int)(newNode: String => RaftNode[A])(implicit s: Scheduler) = {
    val nodesNames = (1 to nrNodes).map { id => s"node $id"
    }.toSet

    val nodes: List[MonixNode[A]] = nodesNames.toList.map { name =>
      val cluster = RaftCluster(nodesNames - name)
      val node: RaftNode[A] = newNode(name).withCluster(cluster)
      val (source, feed) = Pipe.publishToOne[(NodeId, RequestOrResponse[A])].unicast
      new MonixNode[A](node, source, feed)
    }

    val byId: Map[NodeId, MonixNode[A]] = nodes.groupBy(_.id).mapValues(_.head)
    byId.values.foreach(_.subscribeToCluster(byId))

    byId
  }
}
