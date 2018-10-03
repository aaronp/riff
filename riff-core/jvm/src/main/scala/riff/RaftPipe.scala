package riff
import java.nio.file.Path

import eie.io.{FromBytes, ToBytes, _}
import org.reactivestreams.{Publisher, Subscriber}
import riff.raft.log.RaftLog
import riff.raft.messages.{AddressedMessage, RaftMessage, ReceiveHeartbeatTimeout}
import riff.raft.node.{RaftNodeResult, _}
import riff.raft.reactive.ReactiveClient
import riff.raft.timer.{RaftClock, RandomTimer, Timers}
import riff.raft.{NodeId, RaftClient, node}
import riff.reactive.AsPublisher.syntax._
import riff.reactive.{ReactiveTimerCallback, _}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.Properties

/**
  * The generic representation of all the relevant parts for a single node in the raft cluster.
  *
  * This would be the 'builder' for a raft node
  *
  * It contains the underlying, NON-THREAD SAFE, node. Although this could be considered breaking encapsulation,
  * it is included in order to obtain access the underlying components (the log, persistent state, cluster, etc).
  *
  */
class RaftPipe[A, Sub[_]: AsSubscriber, Pub[_]: AsPublisher, C[_]] private (
  val node: RaftNode[A], //
  private[riff] val pipe: ReactivePipe[RaftMessage[A], RaftNodeResult[A], Sub, Pub], //
  val client: RaftClient[C, A]
) {
  def input: Sub[RaftMessage[A]] = pipe.input

  def resetReceiveHeartbeat() = {
    node.resetReceiveHeartbeat()
  }
  def asPublisher: AsPublisher[Pub] = AsPublisher[Pub]
  def asSubscriber: AsSubscriber[Sub] = AsSubscriber[Sub]

  def nodeId: NodeId = node.nodeId

  /**
    *
    * @param targetNodeId the recipient node for which any messages will be filtered/sent to
    * @return a publisher of messages from this pipe to the target node
    */
  def publisherFor(targetNodeId: NodeId): Publisher[RaftMessage[A]] = inputFor(targetNodeId).asPublisher

  def inputFor(targetNodeId: NodeId): Pub[RaftMessage[A]] = {
    require(targetNodeId != node.nodeId, s"Attempted loop - can't create an input from $targetNodeId to itself")

    object MessageTo {
      def unapply(result: RaftNodeResult[A]): Option[AddressedMessage[A]] = {
        result match {
          case AddressedRequest(requests) =>
            //
            // here a message from 'peerId' is sending a message to 'targetId',
            // so we translate that into an AddressedMessage FROM peerId
            //
            val filtered: Iterable[AddressedMessage[A]] = requests.collect {
              case (`targetNodeId`, msg) => AddressedMessage[A](nodeId, msg)
            }

            // At this point we've taken the single output result from a node and filtered it out on
            // messages to 'us' (where 'us' is this peer). And so, if one input to a RaftNode results
            // in more than one messages to another node that's a bug
            filtered.toList match {
              case List(toUs) => Option(toUs)
              case Nil => None
              case many => sys.error(s"${many.size} messages were sent to $targetNodeId from $nodeId: ${many}")
            }
          case AddressedResponse(`targetNodeId`, msg) => Option(AddressedMessage[A](nodeId, msg))
          case _ => None
        }
      }
    }

    pipe.output.collect {
      case MessageTo(request) => request
    }
  }
}

object RaftPipe {

  /**
    * Create a cluster of the input nodes, so that each knows about its peers, and sends messages to/receives messages from each of them
    *
    * @param nodes
    * @param execCtxt
    * @tparam A
    * @return
    */
  def asCluster[A: ClassTag](nodes: RaftNode[A]*)(
    implicit execCtxt: ExecutionContext): Map[NodeId, RaftPipe[A, Subscriber, Publisher, Publisher]] = {

    val ids = nodes.map(_.nodeId).toSet
    def duplicates: Set[NodeId] = nodes.groupBy(_.nodeId).filter(_._2.size > 1).keySet
    require(ids.size == nodes.length, s"multiple nodes given w/ the same id: '${duplicates.mkString("[", ",", "]")}'")

    val raftInstById: Map[NodeId, RaftPipe[A, Subscriber, Publisher, Publisher]] = nodes.map { n =>
      val timer = ReactiveTimerCallback()
      val node = n.withCluster(RaftCluster(ids - n.nodeId)).withTimerCallback(timer)

      val raftPipe = raftPipeForNode(node)

      timer.subscribe(raftPipe.input)
      node.nodeId -> raftPipe
    }.toMap

    wireTogether[A, Subscriber, Publisher, Publisher](raftInstById)

    raftInstById
  }

  def apply[A: FromBytes: ToBytes: ClassTag](id: NodeId = "single-node-cluster")(
    implicit ctxt: ExecutionContext): RaftPipe[A, Subscriber, Publisher, Publisher] = {
    val dataDir = Properties.tmpDir.asPath.resolve(s".riff/${id.filter(_.isLetterOrDigit)}")
    apply(id, dataDir)
  }

  def apply[A: FromBytes: ToBytes: ClassTag](id: NodeId, dataDir: Path)(
    implicit ctxt: ExecutionContext): RaftPipe[A, Subscriber, Publisher, Publisher] = {
    implicit val clock: RaftClock = RaftClock(250.millis, RandomTimer(1.second, 2.seconds))
    val timer = ReactiveTimerCallback()

    val node: RaftNode[A] = newSingleNode(id, dataDir).withTimerCallback(timer)
    val pipe = pipeForNode(node)

    timer.subscribe(pipe.input)

    val client: RaftClient[Publisher, A] = ReactiveClient(node)
    new RaftPipe(node, pipe, client)
  }

  /** convenience method for created a default [[RaftNode]] which has an empty cluster
    *
    * @return a new RaftNode
    */
  def newSingleNode[A: FromBytes: ToBytes](id: NodeId, dataDir: Path, maxAppendSize: Int = 10)(
    implicit clock: RaftClock): RaftNode[A] = {
    new RaftNode[A](
      NIOPersistentState(dataDir.resolve("state"), true).cached(),
      RaftLog[A](dataDir.resolve("data"), true),
      new Timers(clock),
      RaftCluster(Nil),
      node.FollowerNodeState(id, None),
      maxAppendSize
    )
  }

  private[riff] def raftPipeForNode[A: ClassTag](node: RaftNode[A])(
    implicit executionContext: ExecutionContext): RaftPipe[A, Subscriber, Publisher, Publisher] = {
    val pipe = pipeForNode(node)

    val client: RaftClient[Publisher, A] = ReactiveClient(node)

    new RaftPipe[A, Subscriber, Publisher, Publisher](node, pipe, client)
  }

  /**
    * wraps the [[RaftNode]] in a pipe (stream)
    *
    * @param node the node to handle the pipe's input
    * @param executionContext the context used for the stream
    * @tparam A
    * @return a pipe wrapping the node
    */
  private[riff] def pipeForNode[A](node: RaftNode[A])(implicit executionContext: ExecutionContext)
    : ReactivePipe[RaftMessage[A], RaftNodeResult[A], Subscriber, Publisher] = {
    val original = ReactivePipe.multi[RaftMessage[A]](1000, true)
    import AsPublisher.syntax._
    val newOut: Publisher[RaftNodeResult[A]] = original.output.map { msg => //
      println(s"\t\t${node.nodeId} receiving $msg")
      node.onMessage(msg)
    }
    //val bp = BatchedPublisher(newOut, 10, 100)
    ReactivePipe(original.input, newOut)
  }

  def wireTogether[A, Sub[_]: AsSubscriber, Pub[_]: AsPublisher, C[_]](
    raftInstById: Map[NodeId, RaftPipe[A, Sub, Pub, C]]) = {
    import AsSubscriber.syntax._

    /** subscribe each node to the input from its peers
      */
    raftInstById.foreach {
      case (id, instance) =>
        val peers = raftInstById.collect {
          case (peerId, peer) if peerId != id =>
            val pub = instance.publisherFor(peerId)
            pub.subscribe(peer.input.asSubscriber)
        }
        peers.size
    }
  }
}
