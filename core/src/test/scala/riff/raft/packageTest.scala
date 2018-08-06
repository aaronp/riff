package riff.raft
import riff.RiffSpec

class packageTest extends RiffSpec {

  "isMajority" should {
    //format:off
    List(
      (0, 1, false),
      (1, 1, true),
      (0, 2, false),
      (1, 2, false),
      (2, 2, true),
      (0, 3, false),
      (1, 3, false),
      (2, 3, true),
      (3, 3, true),
      (0, 5, false),
      (1, 5, false),
      (2, 5, false),
      (3, 5, true),
      (4, 5, true),
      (5, 5, true)
    ).foreach {
      //format:on

      case (votes, size, true) =>
        s"be leader w/ $votes out of $size" in {
          isMajority(votes, size) shouldBe true
        }
      case (votes, size, false) =>
        s"not be leader w/ $votes out of $size" in {
          isMajority(votes, size) shouldBe false
        }
    }
  }
}
