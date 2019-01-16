package riff.rest

case class RestRequest(method: HttpMethod, uri: String, body: Array[Byte], headers: Map[String, String]) {
  def bodyAsString = new String(body)

  override def toString = s"${method} $uri ${headers.mkString("[", "; ", "]")} { $bodyAsString } "
}
