package streaming.rest

import streaming.api.HostPort

/**
  * Represents an endpoint which may be targeted by a client.
  *
  * @param location the host/port of the service
  * @param uri      the uri to target
  * @param params   a simple map of params. Some may be applied to fill in the uri, and the rest would presumably
  *                 be used to construct query params
  */
case class EndpointCoords(location: HostPort, uri: WebURI, params: Map[String, String]) {
  val resolvedUri: String = uri.resolve(params) match {
    case Left(err)   => sys.error(err)
    case Right(path) => path.mkString("/", "/", "")
  }

  def host = location.host

  def port = location.port

  def hostPort = location.hostPort
}

object EndpointCoords {

  def get(hostPort: HostPort, uri: String, params: Map[String, String] = Map.empty): EndpointCoords =
    EndpointCoords(hostPort, WebURI(HttpMethod.GET, uri), params)
  def post(hostPort: HostPort, uri: String, params: Map[String, String] = Map.empty): EndpointCoords =
    EndpointCoords(hostPort, WebURI(HttpMethod.POST, uri), params)
  def put(hostPort: HostPort, uri: String, params: Map[String, String] = Map.empty): EndpointCoords =
    EndpointCoords(hostPort, WebURI(HttpMethod.PUT, uri), params)
  def delete(hostPort: HostPort, uri: String, params: Map[String, String] = Map.empty): EndpointCoords =
    EndpointCoords(hostPort, WebURI(HttpMethod.DELETE, uri), params)
  def head(hostPort: HostPort, uri: String, params: Map[String, String] = Map.empty): EndpointCoords =
    EndpointCoords(hostPort, WebURI(HttpMethod.HEAD, uri), params)

  def apply(hostPort: HostPort, uri: WebURI, params: (String, String)*): EndpointCoords = EndpointCoords(hostPort, uri, params.toMap)
}
