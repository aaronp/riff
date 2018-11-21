package riff.monix
import java.nio.file.Path

import eie.io.{FromBytes, ToBytes}
import monix.execution.Scheduler
import monix.reactive.{Observable, Observer}
import riff.RaftPipe
import riff.monix.log.ObservableLog
import riff.raft.NodeId
import riff.raft.log.RaftLog
import riff.raft.node._
import riff.raft.timer.{RaftClock, Timers}

import scala.reflect.ClassTag

case class Raft[A: ToBytes: FromBytes: ClassTag](raftNode: RaftNode[A], stateCallback: ObservableState, timerCallback: ObservableTimerCallback, log: ObservableLog[A])(
    implicit val scheduler: Scheduler,
    val clock: RaftClock) {

  lazy val pipe: RaftPipe[A, Observer, Observable, Observable, RaftNode[A]] = {
    val p = RaftPipeMonix.raftPipeForHandler(raftNode)

    require(timerCallback == raftNode.timerCallback, s"${timerCallback} != ${raftNode.timerCallback}")

    timerCallback.subscribe(p.input)

    p
  }
}

object Raft {

  def apply[A: ToBytes: FromBytes: ClassTag](name: NodeId, dir: Path, cluster: RaftCluster, maxAppendSize: Int = 1000, createDirIfNotExists: Boolean = true)(
      implicit scheduler: Scheduler,
      clock: RaftClock): Raft[A] = {

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
    new Raft(raftNode, stateCallback, timerCallback, log)
  }

}
