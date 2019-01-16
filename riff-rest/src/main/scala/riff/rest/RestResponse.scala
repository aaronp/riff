package riff.rest

case class RestResponse(body: Array[Byte], headers: Map[String, String], statusCode: Int = 200) {
  def bodyAsString      = new String(body)
  override def toString = s"RestResponse(statusCode=$statusCode, headers=$headers, body=$bodyAsString)"
}

object RestResponse {
  def json(body: String): RestResponse = RestResponse(body.getBytes("UTF-8"), Map("content-type" -> "application/json"))
  def text(body: String): RestResponse = RestResponse(body.getBytes("UTF-8"), Map("content-type" -> "text/plain"))
}
