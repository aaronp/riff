package streaming.rest
import eie.io.ToBytes

sealed trait RestInput {
  def uri: WebURI

  def headers: Map[String, String]

  def bodyAsBytes: Array[Byte] = this match {
    case content: RestInput.ContentInput[_] => content.bytes
    case _                                  => Array.empty[Byte]
  }
}

object RestInput {
  def apply(uri: WebURI, headers: Map[String, String] = Map.empty) = BasicInput(uri, headers)

  def apply[A: ToBytes](uri: WebURI, body: A, headers: Map[String, String]) = ContentInput(uri, body, headers)

  case class BasicInput(uri: WebURI, headers: Map[String, String]) extends RestInput

  case class ContentInput[A: ToBytes](uri: WebURI, body: A, headers: Map[String, String]) extends RestInput {
    def bytes: Array[Byte] = ToBytes[A].bytes(body)
  }
}
