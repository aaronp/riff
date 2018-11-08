package riff.monix
import java.nio.file.Path

import eie.io._
import monix.execution.Scheduler
import monix.reactive.observables.ConnectableObservable
import monix.reactive.{Observable, Observer, Pipe}
import riff.RaftPipe
import riff.RaftPipe.wireTogether
import riff.raft.log.LogAppendSuccess
import riff.raft.messages.{AppendData, RaftMessage}
import riff.raft.node._
import riff.raft.timer.{RaftClock, RandomTimer}
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

    val ids = nodes.map(_.nodeId).toSet
    def duplicates: Set[NodeId] = nodes.groupBy(_.nodeId).filter(_._2.size > 1).keySet
    require(ids.size == nodes.length, s"multiple nodes given w/ the same id: '${duplicates.mkString("[", ",", "]")}'")

    val raftInstById: Map[NodeId, RaftPipe[A, Observer, Observable, Observable, RaftNode[A]]] = nodes.map { n =>
      val timer = new ObservableTimerCallback

      val node = n.withCluster(RaftCluster(ids - n.nodeId)).withTimerCallback(timer).withRoleCallback(new ObservableState)

      val raftPipe = raftPipeForNode[A, RaftNode[A]](node)

      timer.subscribe(raftPipe.input)
      node.nodeId -> raftPipe
    }.toMap

    wireTogether[A, Observer, Observable, Observable, RaftNode[A]](raftInstById)

    raftInstById
  }

  def apply[A: FromBytes: ToBytes: ClassTag](id: NodeId = "single-node-cluster")(implicit sched: Scheduler): RaftPipe[A, Observer, Observable, Observable, RaftNode[A]] = {
    val dataDir = Properties.tmpDir.asPath.resolve(s".riff/${id.filter(_.isLetterOrDigit)}")
    apply(id, dataDir)
  }

  def apply[A: FromBytes: ToBytes: ClassTag](id: NodeId, dataDir: Path)(implicit sched: Scheduler): RaftPipe[A, Observer, Observable, Observable, RaftNode[A]] = {

    implicit val clock: RaftClock = RaftClock(250.millis, RandomTimer(1.second, 2.seconds))
    val timer = new ObservableTimerCallback

    val node = RaftPipe.newSingleNode(id, dataDir).withTimerCallback(timer).withRoleCallback(new ObservableState)
    val pipe: RaftPipe[A, Observer, Observable, Observable, RaftNode[A]] = raftPipeForNode(node)

    timer.subscribe(pipe.input)

    pipe
  }

  def raftPipeForNode[A: ClassTag, Handler <: RaftMessageHandler[A]](handler: Handler)(implicit sched: Scheduler): RaftPipe[A, Observer, Observable, Observable, Handler] = {

    val pipe: ReactivePipe[RaftMessage[A], RaftNodeResult[A], Observer, Observable] = pipeForNode[A](handler)

    new RaftPipe[A, Observer, Observable, Observable, Handler](handler, pipe, RiffMonixClient(pipe.input))
  }

  /**
    * wraps the [[RaftNode]] in a pipe (stream)
    *
    * @param node the node to handle the pipe's input
    * @param executionContext the context used for the stream
    * @tparam A
    * @return a pipe wrapping the node
    */
  private[riff] def pipeForNode[A](originalHandler: RaftMessageHandler[A], debug: Boolean = true)(
    implicit sched: Scheduler): ReactivePipe[RaftMessage[A], RaftNodeResult[A], Observer, Observable] = {

    val (originalInput, originalOutput) = Pipe.publish[RaftMessage[A]].multicast

    // for tests
    val node = if (debug) {
      RecordingMessageHandler(originalHandler)
    } else {
      originalHandler
    }

    // provide a publisher of the inputs w/ their outputs from the node.
    // this way we can more easily satisfy raft clients'
    val zippedInput = originalOutput.map { input =>
      val output = node.onMessage(input)
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
            val statusPub: ConnectableObservable[AppendStatus] = {
              AppendStatus.asStatusPublisher(node.nodeId, clusterSize, logAppendSuccess, zippedInput).replay
            }
            statusPub.subscribe(Observer.fromReactiveSubscriber(append.statusSubscriber, statusPub.connect()))

          case err: Exception =>
            Publishers.InError(err).subscribe(append.statusSubscriber)
        }
        output
      case (_, output) => output
    }

    ReactivePipe(originalInput, nodeOutput)
  }

}
