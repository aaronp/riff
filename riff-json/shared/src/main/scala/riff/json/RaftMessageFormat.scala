package riff.json
import io.circe.Decoder.Result
import io.circe._
import io.circe.syntax._
import riff.raft.messages._

class RaftMessageFormat[NodeKey, A](implicit nodeKeyEnc: Encoder[NodeKey], nodeKeyDec: Decoder[NodeKey], logEnc: Encoder[A], logDec: Decoder[A])
    extends Encoder[RaftMessage[NodeKey, A]]
    with Decoder[RaftMessage[NodeKey, A]] {
  private val SendHeartbeatTimeoutName    = "SendHeartbeatTimeout"
  private val ReceiveHeartbeatTimeoutName = "ReceiveHeartbeatTimeout"

  override def apply(input: RaftMessage[NodeKey, A]): Json = {
    input match {
      case msg: AppendEntries[A] =>
        import io.circe.generic.auto._
        val entries = Json.arr(msg.entries.map(_.asJson): _*)
        Json.obj(
          "AppendEntries" ->
            Json.obj(
              "previous"    -> msg.previous.asJson,
              "term"        -> msg.term.asJson,
              "commitIndex" -> msg.commitIndex.asJson,
              "entries"     -> entries
            ))
      case msg: RequestVote =>
        import io.circe.generic.auto._
        Json.obj("RequestVote" -> msg.asJson)
      case ReceiveHeartbeatTimeout => Json.fromString(ReceiveHeartbeatTimeoutName)
      case SendHeartbeatTimeout    => Json.fromString(SendHeartbeatTimeoutName)
      case msg: RequestVoteResponse =>
        import io.circe.generic.auto._
        Json.obj("RequestVoteResponse" -> msg.asJson)
      case msg: AppendEntriesResponse =>
        import io.circe.generic.auto._
        Json.obj("AppendEntriesResponse" -> msg.asJson)
    }
  }
  override def apply(c: HCursor): Result[RaftMessage[NodeKey, A]] = {
    c.as[String] match {
      case Right(SendHeartbeatTimeoutName)    => Right(SendHeartbeatTimeout)
      case Right(ReceiveHeartbeatTimeoutName) => Right(ReceiveHeartbeatTimeout)
      case Right(other) =>
        val fail = DecodingFailure(s"Invalid json string '$other', expected one of $ReceiveHeartbeatTimeoutName or $SendHeartbeatTimeoutName", c.history)
        Left(fail)
      case Left(_) =>
        import io.circe.generic.auto._

        val opt = c
          .downField("AppendEntries")
          .success
          .map(decodeAppendEntries)
          .orElse(c.downField("AppendEntriesResponse").success.map(_.as[AppendEntriesResponse]))
          .orElse(c.downField("RequestVoteResponse").success.map(_.as[RequestVoteResponse]))
          .orElse(c.downField("RequestVote").success.map(_.as[RequestVote]))

        opt.getOrElse(Left(DecodingFailure(s"Invalid json string '${c.focus}'", c.history)))
    }
  }
  def decodeAppendEntries(hcursor: HCursor): Result[RaftMessage[NodeKey, A]] = {
    import io.circe.generic.auto._
    hcursor.as[AppendEntries[A]]
  }
}
