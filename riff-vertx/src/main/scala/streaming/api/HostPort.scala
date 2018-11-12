package streaming.api

case class HostPort(host: String, port: Int, secure: Boolean = false) {
  def hostPort = s"$host:$port"

  override def toString = hostPort

  /**
    * @return 'localhost:port' if the host is set to 0.0.0.0 then use, this instance otherwise
    */
  def resolveLocalhost = host match {
    case "0.0.0.0" => HostPort("localhost", port)
    case _ => this
  }

  def asURL = s"http://$hostPort"

  def asWebsocketURL = {
    if (secure) {
      s"wss://${hostPort}"
    } else {
      s"ws://${hostPort}"
    }
  }
}

object HostPort {

  def local(port: Int): HostPort = HostPort("0.0.0.0", port)
  def localhost(port: Int): HostPort = HostPort("localhost", port)

  private val HostPortR = "(.*):(\\d+)".r

  def unapply(id: String): Option[HostPort] = {
    id match {
      case HostPortR(h, p) => Option(HostPort(h, p.toInt))
      case _ => None
    }
  }
}
