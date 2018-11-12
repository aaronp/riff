package riff.raft.messages

import riff.RiffSpec

class AppendDataTest extends RiffSpec {

  "AppendData.toString" should {
    "improve our code coverage" in {
      AppendData(1,2,3).toString shouldBe "AppendData([1,2,3])"
      AppendData(1,2,3,4,5,6).toString shouldBe "AppendData(6 values: [1,2,3,4,5,...])"
    }
  }
  "AppendData.hashCode" should {
    "differ based on values" in {
      AppendData(1,2).hashCode() should not be (AppendData(2,1).hashCode())
    }
    "be consistent" in {
      AppendData(1,2).hashCode() shouldBe AppendData(1,2).hashCode()
    }
  }
}
