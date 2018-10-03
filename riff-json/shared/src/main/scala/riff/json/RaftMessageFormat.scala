package riff.json
import io.circe.Decoder.Result
import io.circe._
import io.circe.syntax._
import org.reactivestreams.Subscriber
import riff.raft.AppendStatus
import riff.raft.messages._
import riff.reactive.Subscribers

import scala.reflect.ClassTag

class RaftMessageFormat[A: ClassTag](implicit logEnc: Encoder[A], logDec: Decoder[A])
    extends Encoder[RaftMessage[A]] with Decoder[RaftMessage[A]] {
  private val SendHeartbeatTimeoutName = "SendHeartbeatTimeout"
  private val ReceiveHeartbeatTimeoutName = "ReceiveHeartbeatTimeout"

  private val ReceiveHBJson = Json.fromString(ReceiveHeartbeatTimeoutName)
  private val SendHBJson = Json.fromString(SendHeartbeatTimeoutName)
  override def apply(input: RaftMessage[A]): Json = {
    input match {
      case AppendData(_, values : Array[A]) =>
        val array = Json.arr(values.map(_.asJson): _*)
        Json.obj("AppendData" -> array)
      case ReceiveHeartbeatTimeout => ReceiveHBJson
      case SendHeartbeatTimeout => SendHBJson
      case AddressedMessage(from, msg: RequestOrResponse[A]) =>
        Json.obj("from" -> Json.fromString(from), "message" -> requestOrResponseAsJson(msg))
    }
  }

  def requestOrResponseAsJson(input: RequestOrResponse[A]): Json = {
    input match {
      case msg: AppendEntries[A] =>
        import io.circe.generic.auto._
        val entries = Json.arr(msg.entries.map(_.asJson): _*)
        Json.obj(
          "AppendEntries" ->
            Json.obj(
              "previous" -> msg.previous.asJson,
              "term" -> msg.term.asJson,
              "commitIndex" -> msg.commitIndex.asJson,
              "entries" -> entries
            ))
      case msg: RequestVote =>
        import io.circe.generic.auto._
        Json.obj("RequestVote" -> msg.asJson)
      case msg: RequestVoteResponse =>
        import io.circe.generic.auto._
        Json.obj("RequestVoteResponse" -> msg.asJson)
      case msg: AppendEntriesResponse =>
        import io.circe.generic.auto._
        Json.obj("AppendEntriesResponse" -> msg.asJson)
    }
  }
  override def apply(c: HCursor): Result[RaftMessage[A]] = {
    c.as[String] match {
      case Right(SendHeartbeatTimeoutName) => Right(SendHeartbeatTimeout)
      case Right(ReceiveHeartbeatTimeoutName) => Right(ReceiveHeartbeatTimeout)
      case Right(other) =>
        val fail = DecodingFailure(
          s"Invalid json string '$other', expected one of $ReceiveHeartbeatTimeoutName or $SendHeartbeatTimeoutName",
          c.history)
        Left(fail)
      case Left(_) =>
        import io.circe.generic.auto._

        def asRequestOrResponse: Either[DecodingFailure, RequestOrResponse[A]] = {
          val msgField = c.downField("message")
          val opt = msgField.downField("AppendEntries").success.map(decodeAppendEntries)
            .orElse(msgField.downField("AppendEntriesResponse").success.map(_.as[AppendEntriesResponse]))
            .orElse(msgField.downField("RequestVoteResponse").success.map(_.as[RequestVoteResponse]))
            .orElse(msgField.downField("RequestVote").success.map(_.as[RequestVote]))

          opt.getOrElse(Left(DecodingFailure(s"Invalid json message '${c.focus}'", c.history)))
        }

        val addressed: Either[DecodingFailure, AddressedMessage[A]] = for {
          from <- c.downField("from").as[String]
          msg <- asRequestOrResponse
        } yield {
          AddressedMessage(from, msg)
        }

        addressed match {
          case Left(_) => c.downField("AppendData").as[Array[A]].map { array: Array[A] =>
            new AppendData[A, Subscriber](Subscribers.NoOp[AppendStatus](), array)
          }
          case right => right
        }
    }
  }

  def decodeAppendEntries(hcursor: HCursor): Result[AppendEntries[A]] = {
    import io.circe.generic.auto._
    hcursor.as[AppendEntries[A]]
  }
}
