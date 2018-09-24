package riff.raft.log
import riff.RiffSpec
import riff.raft.messages.AppendEntriesResponse

class LogAppendSuccessTest extends RiffSpec {

  "LogAppendSuccess.contains" should {
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
