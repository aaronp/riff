package riff.monix
import java.nio.file.Path

import eie.io._
import monix.execution.{Cancelable, Scheduler}
import monix.reactive.{Observable, Observer, Pipe}
import riff.RaftPipe
import riff.monix.client.{AppendStatusObservable, MonixClient}
import riff.monix.log.ObservableLog
import riff.raft.log.{LogAppendResult, RaftLog}
import riff.raft.messages.{AppendData, RaftMessage}
import riff.raft.node.{NodeAppendResult, _}
import riff.raft.timer.{RaftClock, RandomTimer, Timers}
import riff.raft.{AppendStatus, NodeId}
import riff.reactive._

import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.Properties

object RaftPipeMonix extends LowPriorityRiffMonixImplicits {

  def inMemoryClusterOf[A: ClassTag](size: Int)(implicit sched: Scheduler, clock: RaftClock): Map[NodeId, RaftPipe[A, Observer, Observable, Observable, RaftNode[A]]] = {
    val nodes = (1 to size).map { i => //
      RaftNode.inMemory[A](s"node-$i")
    }
    asCluster(nodes: _*)
  }

  /**
    * Create a cluster of the input nodes, so that each knows about its peers, and sends messages to/receives messages from each of them
    *
    * @param nodes
    * @param execCtxt
    * @tparam A
    * @return
    */
  def asCluster[A: ClassTag](nodes: RaftNode[A]*)(implicit sched: Scheduler): Map[NodeId, RaftPipe[A, Observer, Observable, Observable, RaftNode[A]]] = {

    val ids                     = nodes.map(_.nodeId).toSet
    def duplicates: Set[NodeId] = nodes.groupBy(_.nodeId).filter(_._2.size > 1).keySet
    require(ids.size == nodes.length, s"multiple nodes given w/ the same id: '${duplicates.mkString("[", ",", "]")}'")

    val raftInstById: Map[NodeId, RaftPipe[A, Observer, Observable, Observable, RaftNode[A]]] = nodes.map { n =>
      val raftPipe = observablePipeForNode(n.withCluster(RaftCluster(ids - n.nodeId)))
      raftPipe.nodeId -> raftPipe
    }.toMap

    wireTogetherMonix(raftInstById)

    raftInstById
  }

  def singleNode[A: FromBytes: ToBytes: ClassTag](id: NodeId = "single-node-cluster")(implicit sched: Scheduler): RaftPipe[A, Observer, Observable, Observable, RaftNode[A]] = {
    val dataDir = Properties.tmpDir.asPath.resolve(s".riff/${id.filter(_.isLetterOrDigit)}")
    singleNode(id, dataDir)
  }

  def singleNode[A: FromBytes: ToBytes: ClassTag](id: NodeId, dataDir: Path)(implicit sched: Scheduler): RaftPipe[A, Observer, Observable, Observable, RaftNode[A]] = {

    implicit val clock: RaftClock = RaftClock(250.millis, RandomTimer(1.second, 2.seconds))
    val timer                     = new ObservableTimerCallback

    val node                                                             = newSingleNode(id, dataDir).withTimerCallback(timer).withRoleCallback(new ObservableState)
    val pipe: RaftPipe[A, Observer, Observable, Observable, RaftNode[A]] = raftPipeForHandler(node)

    timer.subscribe(pipe.nodeId, pipe.input)

    pipe
  }

  /** convenience method for created a default [[RaftNode]] which has an empty cluster
    *
    * @return a new RaftNode
    */
  def newSingleNode[A: FromBytes: ToBytes](id: NodeId, dataDir: Path, maxAppendSize: Int = 10)(implicit clock: RaftClock): RaftNode[A] = {
    new RaftNode[A](
      NIOPersistentState(dataDir.resolve("state"), true).cached(),
      RaftLog[A](dataDir.resolve("data"), true),
      new Timers(clock),
      RaftCluster(Nil),
      FollowerNodeState(id, None),
      maxAppendSize
    )
  }

  def observablePipeForNode[A: ClassTag](n: RaftNode[A])(implicit sched: Scheduler): RaftPipe[A, Observer, Observable, Observable, RaftNode[A]] = {
    val timer = new ObservableTimerCallback

    val node: RaftNode[A] = {
      n.withTimerCallback(timer) //
        .withLog(ObservableLog(n.log)) //
        .withRoleCallback(new ObservableState)
    }

    val raftPipe = raftPipeForHandler[A](node)

    timer.subscribe(raftPipe.nodeId, raftPipe.input)
    raftPipe
  }

  /**
    * Wraps the given [[RaftMessageHandler]] in a [[RaftPipe]]
    *
    * @param handler
    * @param sched
    * @tparam A
    * @tparam Handler
    * @return a pipe which invokes the handler to produce its outputs
    */
  def raftPipeForHandler[A: ClassTag](handler: RaftNode[A])(implicit sched: Scheduler): RaftPipe[A, Observer, Observable, Observable, RaftNode[A]] = {
    val pipe: ReactivePipe[RaftMessage[A], RaftNodeResult[A], Observer, Observable] = pipeForNode[A](handler)
    val resultsObs: Observable[LogAppendResult] = handler.log match {
      case obs: ObservableLog[A] => obs.appendResults
      case other =>
        sys.error(s"Misconfiguration issue: We expected the RaftLog to be an ObservableLog so we could correctly implement error cases in append streams, but got: $other")
    }
    val client = MonixClient(pipe.input, resultsObs)
    new RaftPipe[A, Observer, Observable, Observable, RaftNode[A]](handler, pipe, client)
  }

  /**
    * wraps the [[RaftNode]] in a pipe (stream)
    *
    * @param node the node to handle the pipe's input
    * @param executionContext the context used for the stream
    * @tparam A
    * @return a pipe wrapping the node
    */
  private[riff] def pipeForNode[A](handler: RaftNode[A])(implicit sched: Scheduler): ReactivePipe[RaftMessage[A], RaftNodeResult[A], Observer, Observable] = {
    import ObservableLog._
    val observableLog: ObservableLog[A] = handler.log.observable

    val appendHandler = onAppendData(handler, observableLog) _

    pipeForHandler(handler, observableLog, appendHandler)
  }

  /** When handling append requests, create an observable based on the log and node inputs BEFORE
    * sending the append request to the node.
    *
    * That observable is filtered for events specific to the append (which has not yet been applied),
    * and told to 'replay' so that the client can subscribe at any time.
    */
  private[riff] def onAppendData[A](handler: RaftNode[A], observableLog: ObservableLog[A])(responseSubscriber: Observer[AppendStatus],
                                                                                           nodeInput: Observable[RaftMessage[A]],
                                                                                           d8a: Array[A])(implicit sched: Scheduler): RaftNodeResult[A] = {
    val clusterSize             = handler.cluster.numberOfPeers + 1
    val (appendResultVar, feed) = AppendStatusObservable(handler.nodeId, clusterSize, nodeInput, observableLog.appendResults(), observableLog.committedCoords())
    feed.subscribe(responseSubscriber)

    /** Now, having deftly arranged and for the responseSubscriber to listen to the results we're about to poke, we poke...
      */
    val NodeAppendResult(appendResult: LogAppendResult, response: RaftNodeResult[A]) = handler.appendIfLeader(d8a)

    /** ... and let it know what the result of the append is, BEFORE returning the response
      * (which means this event, which we've just created, will have been received BEFORE anything
      * could possibly react to the 'response' which has not yet been pushed to anyone).
      */
    Observer.feed(appendResultVar, List(appendResult))

    response
  }

  private[riff] def pipeForHandler[A, H <: RaftMessageHandler[A]](handler: H,
                                                                  observableLog: ObservableLog[A],
                                                                  onAppendData: (Observer[AppendStatus], Observable[RaftMessage[A]], Array[A]) => RaftNodeResult[A])(
      implicit sched: Scheduler): ReactivePipe[RaftMessage[A], RaftNodeResult[A], Observer, Observable] = {

    /**
      * 1) Ensure any one input failure (e.g. the feed from one of the nodes in the cluster) won't cancel the concurrent
      * input of this node
      */
    val subject = InfiniteConcurrentSubject[RaftMessage[A]]

    // provide a publisher of the inputs w/ their outputs from the node.
    // this way we can more easily satisfy raft clients'
    val out: Observable[RaftNodeResult[A]] = {

      /** 2) Take our concurrent subject and feed that in a single pipe
        */
      val nodeInput: Observable[RaftMessage[A]] = {
        val (singleThreadedIn, nodeIn) = Pipe.publishToOne[RaftMessage[A]].unicast
        subject.output.subscribe(singleThreadedIn)
        nodeIn.share
      }

      /** 3) drive the node from this input
        */
      val handled = nodeInput.map {
        case AppendData(responseSubscriber: Observer[AppendStatus], d8a: Array[A]) =>
          onAppendData(responseSubscriber, nodeInput, d8a)
        case append @ AppendData(_, d8a: Array[A]) =>
          val responseSubscriber = Observer.fromReactiveSubscriber(append.statusSubscriber, Cancelable())
          onAppendData(responseSubscriber, nodeInput, d8a)
        case anyOtherInput =>
          handler.onMessage(anyOtherInput)
      }

      handled.share
    }

    ReactivePipe(subject.input, out)
  }

  def wireTogetherMonix[A, H <: RaftMessageHandler[A]](raftInstById: Map[NodeId, RaftPipe[A, Observer, Observable, Observable, H]])(implicit s: Scheduler) = {

    /** subscribe each node to the input from its peers
      */
    raftInstById.foreach {
      case (id, instance) =>
        val peers = raftInstById.collect {
          case (peerId, peer) if peerId != id =>
            val pub = instance.inputFor(peerId)
            pub.dump(s"${id} ====> $peerId").subscribe(peer.input)
            pub
        }

        peers.size
    }
  }
}
