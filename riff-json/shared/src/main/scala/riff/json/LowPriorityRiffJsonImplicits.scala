package riff.json
import io.circe.{Decoder, Encoder}

import scala.reflect.ClassTag

trait LowPriorityRiffJsonImplicits {

  /**
    * @param logEnd the encoder of the log data type A
    * @param logDec the decore for the log data type A
    * @tparam A
    * @return a message format for serializing to/from json
    */
  implicit def formatter[A: ClassTag](implicit logEnd: Encoder[A], logDec: Decoder[A]): RaftMessageFormat[A] = {
    new RaftMessageFormat[A]
  }
}
