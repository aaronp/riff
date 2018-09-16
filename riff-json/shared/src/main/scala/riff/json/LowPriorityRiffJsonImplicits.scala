package riff.json
import io.circe.{Decoder, Encoder}

trait LowPriorityRiffJsonImplicits {
  implicit def formatter[NodeKey, A](implicit nodeKeyEnc : Encoder[NodeKey], nodeKeyDec : Decoder[NodeKey],
                            logEnd : Encoder[A], logDec : Decoder[A]): RaftMessageFormat[NodeKey, A] = {
    new RaftMessageFormat[NodeKey, A]
  }

}
