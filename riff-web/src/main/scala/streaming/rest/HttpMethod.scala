package streaming.rest

sealed trait HttpMethod

object HttpMethod {
  def unapply(name: String): Option[HttpMethod] = {
    values.find(_.toString.equalsIgnoreCase(name))
  }

  case object PUT extends HttpMethod

  case object POST extends HttpMethod

  case object DELETE extends HttpMethod

  case object GET extends HttpMethod

  case object HEAD extends HttpMethod

  case object OPTIONS extends HttpMethod

  case object TRACE extends HttpMethod

  case object CONNECT extends HttpMethod

  case object PATCH extends HttpMethod

  lazy val values = Set(PUT, POST, DELETE, GET, HEAD, OPTIONS, TRACE, CONNECT, PATCH)

}
