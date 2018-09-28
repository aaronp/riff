package riff.json
import io.circe.syntax._
import riff.RiffSpec
import riff.raft.log.{LogCoords, LogEntry}
import riff.raft.messages._

class RaftMessageFormatTest extends RiffSpec {
  case class SomeLogType(x: Int)
  implicit val someLogTypeDec = io.circe.generic.auto.exportDecoder[SomeLogType]
  implicit val someLogTypeEnc = io.circe.generic.auto.exportEncoder[SomeLogType]
  import implicits._

  "importing riff.json.implicits._" should {
    "be able encode/decode ReceiveHeartbeatTimeout" in {
      val expected: RaftMessage[String] = ReceiveHeartbeatTimeout
      expected.asJson.as[RaftMessage[String]] shouldBe Right(ReceiveHeartbeatTimeout)
    }
    "be able encode/decode SendHeartbeatTimeout" in {
      val expected: RaftMessage[String] = SendHeartbeatTimeout
      expected.asJson.as[RaftMessage[String]] shouldBe Right(SendHeartbeatTimeout)
    }
    "be able encode/decode RequestVote" in {
      val expected: RaftMessage[String] = RequestVote(12, LogCoords(3,4))
      expected.asJson.as[RaftMessage[String]] shouldBe Right(RequestVote(12, LogCoords(3,4)))
    }
    "be able encode/decode RequestVoteResponse" in {
      val expected: RaftMessage[String] = RequestVoteResponse(1, false)
      expected.asJson.as[RaftMessage[String]] shouldBe Right(RequestVoteResponse(1, false))
    }
    "be able encode/decode AppendEntriesResponse" in {
      val expected: RaftMessage[String] = AppendEntriesResponse.ok(1, 2)
      expected.asJson.as[RaftMessage[String]] shouldBe Right(AppendEntriesResponse.ok(1, 2))
    }
    "be able encode/decode AppendEntries" in {
      val expected: RaftMessage[SomeLogType] =
        AppendEntries[SomeLogType](LogCoords(3, 4), 5, 6, Array(LogEntry(10, SomeLogType(100)), LogEntry(11, SomeLogType(101))))
      expected.asJson.as[RaftMessage[SomeLogType]] shouldBe Right(expected)
    }
    "be able encode/decode empty AppendEntries" in {
      val expected: RaftMessage[SomeLogType] = AppendEntries[SomeLogType](LogCoords(3, 4), 5, 6)
      expected.asJson.as[RaftMessage[SomeLogType]] shouldBe Right(expected)
    }
  }

}
