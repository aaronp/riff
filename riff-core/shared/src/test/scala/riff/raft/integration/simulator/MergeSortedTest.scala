package riff.raft.integration.simulator
import riff.RiffSpec

class MergeSortedTest extends RiffSpec {

  "MergeSorted" should {
    "merge two already sorted lists" in {
      val list1  = List(1, 2, 3, 7, 10, 11, 12, 15, 16, 19, 20).map(_ -> "left")
      val list2  = List(2, 3, 9, 10, 14, 15, 16, 22, 23).map(_ -> "right")
      val result = MergeSorted(list1, list2)(Ordering.by[(Int, String), Int](_._1))
      result shouldBe
        List(
          (1, "left"),
          (2, "left"),
          (2, "right"),
          (3, "left"),
          (3, "right"),
          (7, "left"),
          (9, "right"),
          (10, "left"),
          (10, "right"),
          (11, "left"),
          (12, "left"),
          (14, "right"),
          (15, "left"),
          (15, "right"),
          (16, "left"),
          (16, "right"),
          (19, "left"),
          (20, "left"),
          (22, "right"),
          (23, "right")
        )
    }
  }
}
