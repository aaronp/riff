package riff.json
import riff.RiffSpec
import riff.raft.log.{LogCoords, LogEntry}
import riff.raft.messages.AppendEntries

class RaftMessageFormatTest extends RiffSpec {

  "RaftMessageFormat" should {
    "be able encode/decode vote responses" in {

      case class SomeLogType(x: Int)
      import io.circe.generic.auto._
      import io.circe.syntax._

      val expected = AppendEntries[SomeLogType](LogCoords(3, 4), 5, 6, Array(LogEntry(10, SomeLogType(100)), LogEntry(11, SomeLogType(101))))
      val json     = expected.asJson

      println(json)

      json.as[AppendEntries[SomeLogType]] shouldBe Right(expected)
    }
  }
}
