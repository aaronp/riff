package riff.raft.rest
import riff.monix.RiffMonixSpec
import riff.raft.log.RaftLog
import riff.monix.log.ObservableLog._

class GetLogStatusTest extends RiffMonixSpec {

  "GetLogStatus" should {
    "work" in {
      withScheduler { implicit s =>


        val log = RaftLog.inMemory[String]().observable
        val instanceUnderTest = GetLogStatus.future(log)
        val initialStatus = instanceUnderTest.getLogStatus().futureValue
      }
    }
  }
}
