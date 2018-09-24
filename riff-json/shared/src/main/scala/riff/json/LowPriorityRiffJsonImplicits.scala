package riff.json
import io.circe.{Decoder, Encoder}

import scala.reflect.ClassTag

trait LowPriorityRiffJsonImplicits {
  implicit def formatter[A: ClassTag](implicit logEnd : Encoder[A], logDec : Decoder[A]): RaftMessageFormat[A] = {
    new RaftMessageFormat[A]
  }
}
