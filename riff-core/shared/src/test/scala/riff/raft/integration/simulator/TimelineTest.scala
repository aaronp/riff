package riff.raft.integration.simulator

import riff.RiffSpec

import scala.concurrent.duration._

class TimelineTest extends RiffSpec {

  "Timeline" should {
    "contain the full timeline" in {
      val Some((timeline, "first")) = Timeline[String]()
        .insertAfter(1.second, "first")
        ._1
        .insertAfter(2.second, "second")
        ._1
        .insertAfter(3.second, "third")
        ._1
        .insertAfter(4.second, "fourth")
        ._1
        .pop()

      timeline.currentTime shouldBe 1000

      val Some((twoLeft, "second")) = timeline.remove((4000L, "fourth")).pop()
      twoLeft.pretty() shouldBe """-1000ms : first
                                  |@0ms    : second
                                  |+1000ms : third
                                  |+2000ms : (removed) fourth
                                  |""".stripMargin

      twoLeft.historyDescending shouldBe List(2000 -> "second", 1000 -> "first")
      twoLeft.currentTime shouldBe 2000
    }
    "insert events at the same time puts them in insertion order" in {
      val timeline = Timeline[String]().
        insertAfter(3.millis, "first")._1
        .insertAfter(3.millis, "second")._1
        .insertAfter(2.millis, "zero")._1
      timeline.events.map(_._2) shouldBe List("zero", "first", "second")
    }
    "insert events in order" in {
      val timeline = Timeline[String]()
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
