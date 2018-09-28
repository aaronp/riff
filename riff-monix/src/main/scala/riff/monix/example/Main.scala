package riff.monix.example
import java.nio.file.Path

import riff.monix.MonixCluster
import riff.raft.node.RaftNode

object Main extends App {

  def cluster(dir: Path) = {
    MonixCluster.of(4) { name =>
      val dataDir = dir.resolve(name)
      //RaftNode[String](dataDir, name)

      ???
    }
  }

}
