package riff.raft

import java.nio.file.Path

import eie.io.{FromBytes, ToBytes}
import riff.raft.log.RaftLog
import riff.raft.node.{NIOPersistentState, RaftCluster, RaftNode}
import riff.raft.timer.{RaftClock, Timers}

object Raft {

  def apply[A: FromBytes: ToBytes](id: NodeId, dataDir: Path, maxAppendSize: Int = 10)(
    implicit timer: RaftClock): RaftNode[A] = {
    new RaftNode[A](
      NIOPersistentState(dataDir.resolve("state"), true).cached(),
      RaftLog[A](dataDir.resolve("data"), true),
      timer,
      RaftCluster(Nil),
      node.FollowerNodeState(id, None),
      maxAppendSize
    )
  }
}
