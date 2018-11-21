package riff.monix
import java.nio.file.Path

import eie.io._
import monix.execution.{Ack, Cancelable, Scheduler}
import monix.reactive.observers.Subscriber
import monix.reactive.subjects.ConcurrentSubject
import monix.reactive.{Observable, Observer, OverflowStrategy, Pipe}
import riff.RaftPipe
import riff.RaftPipe.wireTogether
import riff.monix.log.ObservableLog
import riff.raft.log.{LogAppendResult, LogAppendSuccess, RaftLog}
import riff.raft.messages.{AddressedMessage, AppendData, AppendEntriesResponse, RaftMessage}
import riff.raft.node._
import riff.raft.timer.{RaftClock, RandomTimer, Timers}
import riff.raft.{AppendStatus, NodeId}
import riff.reactive._

import scala.concurrent.Future
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
      n.nodeId -> raftPipe
    }.toMap

    wireTogether[A, Observer, Observable, Observable, RaftNode[A]](raftInstById)

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

    timer.subscribe(pipe.input)

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

    timer.subscribe(raftPipe.input)
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
  private[riff] def pipeForNode[A](node: RaftMessageHandler[A])(implicit sched: Scheduler): ReactivePipe[RaftMessage[A], RaftNodeResult[A], Observer, Observable] = {

    val consumer: ConcurrentSubject[RaftMessage[A], RaftMessage[A]] = ConcurrentSubject.publish[RaftMessage[A]]

    // provide a publisher of the inputs w/ their outputs from the node.
    // this way we can more easily satisfy raft clients'
    val zippedInput = consumer
      .map { input => //
        (input, node.onMessage(input))
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
      case (append @ AppendData(_, _), output @ NodeAppendResult(err: Exception, _)) =>
        Publishers.InError(err).subscribe(append.statusSubscriber)
        output
      case (AppendData(responseSubscriber: Observer[AppendStatus], _), output @ NodeAppendResult(logAppendSuccess: LogAppendSuccess, requests)) =>
        val (_, statusPub) = asStatusPublisher(node.nodeId, clusterSize = requests.size + 1, logAppendSuccess, zippedInput)
        // we have a monix Observer - lovely days
        statusPub.subscribe(responseSubscriber)
        output
      case (append @ AppendData(_, _), output @ NodeAppendResult(logAppendSuccess: LogAppendSuccess, requests)) =>
        val (cancel, statusPub) = asStatusPublisher(node.nodeId, clusterSize = requests.size + 1, logAppendSuccess, zippedInput)
        // we have some other kind of subscriber - wrap as a monix observable
        val obs = Observer.fromReactiveSubscriber(append.statusSubscriber, cancel)
        statusPub.subscribe(obs)
        output
      case (_, output) => output
    }

    // we have multiple inputs coming in, and multiple consumers (subscribers) on the other end.
    // to ensure a single route through our RaftNode logic, we subscribe the concurrent input
    // to this consumer, then feed a multicast output
    val (middleWareIn, middleWareOut) = Pipe.publishToOne[RaftNodeResult[A]].multicast
    nodeOutput.subscribe(middleWareIn)

    val input = new Observer[RaftMessage[A]] {
      override def onNext(elem: RaftMessage[A]): Future[Ack] = consumer.onNext(elem)
      override def onError(ex: Throwable): Unit = {
        sched.reportFailure(ex)
      }
      override def onComplete(): Unit = {
        // ignore complete
      }
    }
    ReactivePipe(input, middleWareOut)
  }

  /**
    * Create a publisher of [[AppendStatus]] which a client may observe.
    *
    * This forms arguably the essential, primary use-case in using riff, as client code can then take the decisions based
    * on the provided feed to wait for quorum, full replication, or even just this node's initial AppendStatus (e.g. having data only on this leader node),
    * etc.
    *
    *
    * Essentially the publisher will be a filtered stream of the node's input which collects [[AppendEntriesResponse]]s
    * relevant to the [[riff.raft.messages.AppendData]] result which returned the given 'logAppendSuccess'.
    *
    * Given that [[riff.raft.messages.AppendEntries]] requests can send multiple entries in a configurable batch, this
    * function should be able to cope with a 'zero to many' cardinality of [[AppendEntriesResponse]]s
    *
    * @param nodeId the nodeId of the leader node whose logAppendSuccess and nodeInput are given, used to populate the AppendStatus
    * @param clusterSize the cluster size, used to populate the AppendStatus entries
    * @param logAppendSuccess the result from this leader's log append (as a result of having processed an [[riff.raft.messages.AppendData]] request
    * @param nodeInput the zipped input of request/responses for a target node (which ultimately feeds the result)
    * @tparam A
    * @return a publisher (presumably) of type Pub of AppendStatus messages
    */
  def asStatusPublisher[A](nodeId: NodeId, clusterSize: Int, logAppendSuccess: LogAppendSuccess, nodeInput: Observable[(RaftMessage[A], RaftNodeResult[A])])(
      implicit scheduler: Scheduler): (Cancelable, Observable[AppendStatus]) = {
    val firstStatus: AppendStatus = {
      val appendMap = Map[NodeId, AppendEntriesResponse](nodeId -> AppendEntriesResponse.ok(logAppendSuccess.firstIndex.term, logAppendSuccess.lastIndex.index))
      AppendStatus(logAppendSuccess, appendMap, logAppendSuccess.appendedCoords, clusterSize)
    }

    val updates = nodeInput.asyncBoundary(OverflowStrategy.BackPressure(2)).scan(firstStatus) {
      case (currentStatus, (AddressedMessage(from, appendResponse: AppendEntriesResponse), leaderCommitResp: LeaderCommittedResult[A]))
          if logAppendSuccess.contains(appendResponse) =>
        currentStatus.withResult(from, appendResponse, leaderCommitResp.committed)
      case (currentStatus, _) => currentStatus
    }

    // the input 'nodeInput' is an infinite stream (or should be) of messages from peers, so we need to ensure we put in a complete condition
    // unfortunately we have this weird closure over a 'canComplete' because we want the semantics of 'takeWhile plus the first element which returns false'
    val p = LowPriorityRiffMonixImplicits.observableAsPublisher(scheduler)

    val all                           = (firstStatus +: updates)
    val obs: Observable[AppendStatus] = p.takeWhileIncludeLast(all)(!_.isComplete)
    val replay                        = obs.replay
    replay.connect() -> replay
  }
}
