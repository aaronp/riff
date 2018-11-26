package riff.monix
import java.nio.file.Path

import eie.io.{FromBytes, ToBytes}
import monix.execution.Scheduler
import monix.reactive.{Observable, Observer}
import riff.RaftPipe
import riff.monix.log.ObservableLog
import riff.raft.{NodeId, RaftClient}
import riff.raft.log.RaftLog
import riff.raft.node._
import riff.raft.timer.{RaftClock, Timers}

import scala.reflect.ClassTag

/**
  * This should be the main entry for a Raft node when using Monix.
  *
  * It represents the pieces required to communicate with other Raft nodes in the cluster, whether they be backed by
  * monix or not.
  *
  *
  * @param raftNode
  * @param stateCallback
  * @param timerCallback
  * @param log
  * @param ev$1
  * @param ev$2
  * @param ev$3
  * @param scheduler
  * @param clock
  * @tparam A
  */
class ObservableRaftEndpoint[A: ToBytes: FromBytes: ClassTag] private (raftNode: RaftNode[A],
                                                                       val stateCallback: ObservableState,
                                                                       val timerCallback: ObservableTimerCallback,
                                                                       val log: ObservableLog[A])(implicit val scheduler: Scheduler, val clock: RaftClock) {

  def cluster = raftNode.cluster
  def nodeId = raftNode.nodeId

  def cancelHeartbeats(): Unit = {
    raftNode.cancelSendHeartbeat()
    raftNode.cancelReceiveHeartbeat()
  }

  def resetReceiveHeartbeat(): Unit = {
    raftNode.resetReceiveHeartbeat()
  }

  /**
    * The pipe used for input/output from this node
    */
  lazy val pipe: RaftPipe[A, Observer, Observable, Observable, RaftNode[A]] = {
    val p = RaftPipeMonix.raftPipeForHandler(raftNode)

    require(timerCallback == raftNode.timerCallback, s"${timerCallback} != ${raftNode.timerCallback}")

    timerCallback.subscribe(p.input)

    p
  }

  /** @return a RaftClient for this node. It has no inherent recovery characteristics -- if it is uses against this node while
    *         the node is NOT the leader then append requests will return an Observable representing an error
    */
  def client: RaftClient[Observable, A] = pipe.client
}

object ObservableRaftEndpoint {

  /**
    *
    * @param name the name of this node. It will be used in messages such as [[AddressedRequest]] from this and other nodes
    * @param dir the directory in which to store the log data and persistent state
    * @param cluster the cluster representing the nodes in this cluster
    * @param maxAppendSize the maximum number of entries to send when catching-up a log
    * @param createDirIfNotExists flag to determine if the directory (dir) argument should create directories if they don't exist
    * @param scheduler a scheduler used for driving the observables
    * @param clock the clock used for the node timeouts
    * @tparam A
    * @return an instance of an endpoint
    */
  def apply[A: ToBytes: FromBytes: ClassTag](name: NodeId, dir: Path, cluster: RaftCluster, maxAppendSize: Int = 1000, createDirIfNotExists: Boolean = true)(
      implicit scheduler: Scheduler,
      clock: RaftClock): ObservableRaftEndpoint[A] = {

    val dataDir            = dir.resolve(".data")
    val persistentStateDir = dir.resolve(".persistentState")

    val stateCallback: ObservableState         = new ObservableState()
    val timerCallback: ObservableTimerCallback = new ObservableTimerCallback
    val log: ObservableLog[A]                  = ObservableLog(RaftLog(dataDir, createDirIfNotExists))

    val raftNode: RaftNode[A] = new RaftNode[A](
      NIOPersistentState(persistentStateDir, createDirIfNotExists),
      log,
      new Timers(clock),
      cluster,
      FollowerNodeState(name, None),
      maxAppendSize,
      timerCallback,
      stateCallback
    )
    new ObservableRaftEndpoint(raftNode, stateCallback, timerCallback, log)
  }

}
