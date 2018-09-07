package riff.raft.log
import scala.scalajs.js

class JSRaftLogTest extends RaftLogTCK {

  override protected def withLog(test: RaftLog[String] => Unit): Unit = {

    RaftLog[String](Repo[String]())
  }

}
