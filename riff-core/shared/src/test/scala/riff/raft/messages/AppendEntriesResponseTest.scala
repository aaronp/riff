package riff.raft.messages
import riff.RiffSpec

class AppendEntriesResponseTest extends RiffSpec {

  "AppendEntriesResponse" should {
    "not allow us to create failed responses w/ a non-zero match index" in {
      val exp = intercept[Exception] {
        AppendEntriesResponse(3, false, 1)
      }
      exp.getMessage should include("Match index '1' should instead be 0 if success is false")
    }
    "not allow us to create responses w/ negative match indices" in {
      val exp = intercept[Exception] {
        AppendEntriesResponse(3, true, -1)
      }
      exp.getMessage should include("Match index '-1' should never be negative")
    }
  }
}
