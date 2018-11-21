package riff.raft.log
import riff.RiffSpec
import riff.raft.messages.{AppendEntries, AppendEntriesResponse}

class LogAppendSuccessTest extends RiffSpec {

  "LogAppendSuccess.contains" should {
    "return true for first responses" ignore {
      AppendEntries(previous=LogCoords(term=0, index=0), term=1, commitIndex=0, Array(LogEntry(1,"Hello"),LogEntry(1,"World")))
      val aer = AppendEntriesResponse(1,true,2)

    }
    "return true for responses w/ the same term and indices" in {
      val response = LogAppendSuccess(LogCoords(4, 5), LogCoords(4, 6))
      response.contains(AppendEntriesResponse.ok(4, 4)) shouldBe false
      response.contains(AppendEntriesResponse.ok(4, 5)) shouldBe true
      response.contains(AppendEntriesResponse.ok(4, 6)) shouldBe true
      response.contains(AppendEntriesResponse.ok(4, 7)) shouldBe false

      response.contains(AppendEntriesResponse.fail(3)) shouldBe false
      response.contains(AppendEntriesResponse.fail(4)) shouldBe false
    }
  }
}
