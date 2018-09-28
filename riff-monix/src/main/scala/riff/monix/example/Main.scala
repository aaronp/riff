package riff.monix.example
import java.nio.file.Path

import riff.monix.MonixNode
import riff.monix.RiffSchedulers.computation._

object Main extends App {

  def cluster(dir: Path) = {
    MonixNode.of(4) { name =>
      val dataDir = dir.resolve(name)
      //RaftNode[String](dataDir, name)

      ???
    }
  }

}
