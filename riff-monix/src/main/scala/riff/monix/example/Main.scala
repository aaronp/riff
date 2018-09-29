package riff.monix.example
import riff.raft._
import java.nio.file.Path

import riff.monix.{MonixNode, MonixClock}
import riff.monix.RiffSchedulers.computation._
import riff.raft.node.RaftNode

object Main extends App {

  def cluster(dir: Path) = {
    MonixNode.of(4) { name =>
      implicit val timer = MonixClock()
      val dataDir = dir.resolve(name)
      val node = mkNode[String](dataDir, name)
      node
    }
  }

}
