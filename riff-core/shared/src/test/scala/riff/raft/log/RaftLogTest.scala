package riff.raft.log

class RaftLogTest extends RaftLogTCK {

  override protected def withLog(test: RaftLog[String] => Unit): Unit = {
    withClue("in memory") {
      test(RaftLog.inMemory[String]())
    }
    withClue("cached") {
      test(RaftLog.inMemory[String]().cached())
    }
  }

}
