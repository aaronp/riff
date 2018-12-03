package riff
import org.reactivestreams.{Publisher, Subscriber}
import riff.raft._
import riff.raft.log.LogAppendSuccess
import riff.raft.messages.{AddressedMessage, AppendData, RaftMessage}
import riff.raft.node.{RaftNodeResult, _}
import riff.raft.reactive.ReactiveClient
import riff.reactive.AsPublisher.syntax._
import riff.reactive._

import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

/**
  *
  * The generic representation of all the relevant parts for a single node in the raft cluster.
  *
  * This would be the 'builder' for a raft node
  *
  * It contains the underlying, NON-THREAD SAFE, node. Although this could be considered breaking encapsulation,
  * it is included in order to obtain access the underlying components (the log, persistent state, cluster, etc).
  *
  * @param nodeId the node Id, just for some sanity/access and to assert the 'publisherFor' isn't creating a loop from a node to itself
  * @param handler the underlying node logic
  * @param pipe the input/output pipe used for the data flowing into/out from the handler
  * @param client a client which can be used to append data to this node (which will fail if this node isn't the leader)
  */
class RaftPipe[A, Sub[_]: AsSubscriber, Pub[_]: AsPublisher, C[_], H <: RaftMessageHandler[A]](
    val handler: H,
    private[riff] val pipe: ReactivePipe[RaftMessage[A], RaftNodeResult[A], Sub, Pub],
    val client: RaftClient[C, A]
) extends AutoCloseable {
  def input: Sub[RaftMessage[A]]     = pipe.input
  def output: Pub[RaftNodeResult[A]] = pipe.output

  def nodeId = handler.nodeId

  def resetReceiveHeartbeat()(implicit ev: H =:= RaftNode[A]) = {
    ev(handler).resetReceiveHeartbeat()
  }
  def asPublisher: AsPublisher[Pub]   = AsPublisher[Pub]
  def asSubscriber: AsSubscriber[Sub] = AsSubscriber[Sub]

  /**
    *
    * @param targetNodeId the recipient node for which any messages will be filtered/sent to
    * @return a publisher of messages from this pipe to the target node
    */
  def publisherFor(targetNodeId: NodeId): Publisher[RaftMessage[A]] = inputFor(targetNodeId).asPublisher

  /**
    * Convenience method to return a publisher of messages resulting from this handler which are destined for the target node
    *
    * @param targetNodeId
    * @return a publisher of [[RaftMessage]]s from this handler to the targetNodeId
    */
  def inputFor(targetNodeId: NodeId): Pub[RaftMessage[A]] = {
    require(targetNodeId != nodeId, s"Attempted loop - can't create an input from $targetNodeId to itself")

    object MessageTo {
      def unapply(result: RaftNodeResult[A]): Option[AddressedMessage[A]] = {
        // change the message to be 'from' this node
        result.toNode(targetNodeId).map(_.copy(from = nodeId)) match {
          case Seq(toUs) => Option(toUs)
          case Seq()     => None
          case many      => sys.error(s"${many.size} messages were sent to $targetNodeId: ${many}")
        }
      }
    }

    pipe.output.collect {
      case MessageTo(request) => request
    }
  }
  override def close(): Unit = {
    pipe.close()
    handler match {
      case closable: AutoCloseable => closable.close()
      case _                       =>
    }
  }
}

object RaftPipe {

  private[riff] def raftPipeForNode[A: ClassTag, Handler <: RaftMessageHandler[A]](handler: Handler, queueSize: Int)(
      implicit executionContext: ExecutionContext): RaftPipe[A, Subscriber, Publisher, Publisher, Handler] = {
    val pipe: ReactivePipe[RaftMessage[A], RaftNodeResult[A], Subscriber, Publisher] = pipeForNode[A](handler, queueSize)

    new RaftPipe[A, Subscriber, Publisher, Publisher, Handler](handler, pipe, ReactiveClient(pipe.inputSubscriber))
  }

  /**
    * wraps the [[RaftNode]] in a pipe (stream)
    *
    * @param node the node to handle the pipe's input
    * @param executionContext the context used for the stream
    * @tparam A
    * @return a pipe wrapping the node
    */
  private[riff] def pipeForNode[A](originalHandler: RaftMessageHandler[A], queueSize: Int, debug: Boolean = true)(
      implicit executionContext: ExecutionContext): ReactivePipe[RaftMessage[A], RaftNodeResult[A], Subscriber, Publisher] = {
    val original: ReactivePipe[RaftMessage[A], RaftMessage[A], Subscriber, Publisher] = ReactivePipe.multi[RaftMessage[A]](queueSize, true)
    import AsPublisher.syntax._

    // for tests
    val node = if (debug) {
      RecordingMessageHandler(originalHandler)
    } else {
      originalHandler
    }

    // a sanity lock which allows me to concentrate on /move forward the riff (raft) code and not the reactive default
    // impl, which hopefully people replace w/ monix, fs2, etc anyway
    object Lock

    // provide a publisher of the inputs w/ their outputs from the node.
    // this way we can more easily satisfy raft clients'
    val zippedInput: Publisher[(RaftMessage[A], node.Result)] = original.output.map { input =>
      val output = Lock.synchronized {
        node.onMessage(input)
      }
      (input, output)
    }

    // provide publishers for the 'AppendData' subscriptions
    //
    // By ensuring the output of this node goes via this mapped result, we ensure that any resulting messages intended
    // for peer nodes are produced after having first subscribed the subscriber attached to an AppendData request to
    // the original zipped input.
    //
    // the consequence of that is that should eliminate any race-condition between subscribing the this node's input
    // and an input being received from one of the peers. e.g.:
    // 1) append data to this leader
    // 2) send 'AppendEntries' request to a follower
    // 3) receive an 'AppendEntriesResponse' from a follower
    // 4) *bang - broken by race condition of #2 and #3* -- subscribe the AppendData's subscriber to this node's input feed,
    //    having then missed the messages from #3
    val nodeOutput = zippedInput.map {
      case (append @ AppendData(_, _), output @ NodeAppendResult(logAppendResult, requests)) =>
        val clusterSize = requests.size + 1
        logAppendResult match {
          case logAppendSuccess: LogAppendSuccess =>
            val statusPub = asStatusPublisherReplay(node.nodeId, clusterSize, logAppendSuccess, zippedInput)

            statusPub.subscribe(append.statusSubscriber)
          case err: Exception =>
            Publishers.InError(err).subscribe(append.statusSubscriber)
        }
        output
      case (_, output) => output
    }

    ReactivePipe(original.input, nodeOutput)
  }

  private def asStatusPublisherReplay[A, Pub[_]: AsPublisher](nodeId: NodeId,
                                                              clusterSize: Int,
                                                              logAppendSuccess: LogAppendSuccess,
                                                              nodeInput: Pub[(RaftMessage[A], RaftNodeResult[A])])(implicit ec: ExecutionContext): Publisher[AppendStatus] = {

    ???
  }

  def wireTogether[A, Sub[_]: AsSubscriber, Pub[_]: AsPublisher, C[_], H <: RaftMessageHandler[A]](raftInstById: Map[NodeId, RaftPipe[A, Sub, Pub, C, H]]) = {
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
