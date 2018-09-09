package riff.raft.integration

import riff.RiffSpec

import scala.concurrent.duration._

class TimelineTest extends RiffSpec {

  "Timeline" should {
    "insert events in order" in {
      val timeline = Timeline()
      timeline.pop() shouldBe empty

      val newTimeline = timeline.insertAtTime(100, "at 100 ms")

      // assert the popped values
      (0 to 3).foreach { _ =>
        val Some((poppedTimeline, event)) = newTimeline.pop()
        poppedTimeline.pop() shouldBe empty
        poppedTimeline.currentTime shouldBe 100
        event shouldBe "at 100 ms"
      }

      val (twoTimeline, (_, "after 5 ms")) = newTimeline.insertAfter(5.millis, "after 5 ms")

      // assert the new value
      (0 to 3).foreach { _ =>
        val Some((poppedTimeline, event)) = twoTimeline.pop()
        poppedTimeline.currentTime shouldBe 5
        event shouldBe "after 5 ms"

        val Some((poppedTimeline2, event2)) = poppedTimeline.pop()
        poppedTimeline2.currentTime shouldBe 100
        poppedTimeline2.pop() shouldBe empty
        event2 shouldBe "at 100 ms"
      }
    }
  }
}
