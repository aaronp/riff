package riff.raft.log
import scala.util.{Success, Try}

trait StringFormat[A] {
  def asString(value: A): String
  def fromString(value: String): Try[A]
}

object StringFormat {
  def apply[A](implicit inst: StringFormat[A]) = inst

  implicit object Identity extends StringFormat[String] {
    override def asString(value: String): String        = value
    override def fromString(value: String): Try[String] = Success(value)
  }
}
