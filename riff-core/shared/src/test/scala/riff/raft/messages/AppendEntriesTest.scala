package riff.raft.messages
import riff.RiffSpec
import riff.raft.log._

class AppendEntriesTest extends RiffSpec {

  "AppendEntries.toString" should {
    "improve our code coverage in a noddy, underhanded kind of way" in {
      AppendEntries(LogCoords.Empty, 1, 2, Array(LogEntry(4, "five"))).toString shouldBe """AppendEntries(previous=LogCoords(term=0, index=0), term=1, commitIndex=2, 1 entries=[LogEntry(4,five)])"""

      val longAppend = AppendEntries(LogCoords.Empty, 3, 4, (0 to 1000).map(i => LogEntry(i, "five")).toArray).toString
      longAppend shouldBe """AppendEntries(previous=LogCoords(term=0, index=0), term=3, commitIndex=4, 1001 entries=[LogEntry(0,five),LogEntry(1,five),LogEntry(2,five),LogEntry(3,five),...])"""
    }
  }
}
