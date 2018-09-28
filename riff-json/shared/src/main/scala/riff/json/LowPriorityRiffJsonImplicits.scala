package riff.json
import io.circe.{Decoder, Encoder}

trait LowPriorityRiffJsonImplicits {
  implicit def formatter[A](implicit logEnd : Encoder[A], logDec : Decoder[A]): RaftMessageFormat[A] = {
    new RaftMessageFormat[A]
  }
}
