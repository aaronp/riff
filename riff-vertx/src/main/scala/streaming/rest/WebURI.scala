package streaming.rest

import java.net.URI

import streaming.rest.HttpMethod._
import streaming.rest.WebURI._

/** Meant to be used when declaring routes.
  *
  * The various streaming impls should be able to improve on this - it's just meant to be a simple
  * way to declare some basic paths.
  *
  * Some parts of the uri may be placeholders -- whether those placeholders are strings, ints, UUIDs, etc, in the
  * end, they all need to resolve to strings.
  *
  * @param uri the uri parts
  */
case class WebURI(method: HttpMethod, uri: List[Part]) {

  type URI = String

  override def toString = s"$method ${pathString}"

  def pathString = uri.mkString("/", "/", "")

  /**
    * pattern matches
    *
    * @param request the method and URI as a string
    * @return a map of the parsed URI parts
    */
  def unapply(request: (HttpMethod, URI)): Option[Map[String, String]] = {
    request match {
      case (`method`, requestUri) => unapply(requestUri)
      case _                      => None
    }
  }

  /**
    * Extract the parameters from the given request Uri
    *
    * @param requestUri
    * @return the extracted parameters as a map
    */
  def unapply(requestUri: URI): Option[Map[String, String]] = {

    val parts = requestUri.split("/", -1).filterNot(_.isEmpty)
    if (parts.length == uri.size) {
      val pears = (uri zip parts).collect {
        case (ParamPart(name), value) => (name, value)
      }
      Option(pears.toMap)
    } else {
      None
    }
  }

  def resolve(firstParam: (String, String), theRest: (String, String)*): Either[String, WebURI] = {
    resolve((firstParam +: theRest).toMap.ensuring(_.size == theRest.size + 1))
  }

  /**
    * Used to resolve the route uri to a string, used by clients to fill-in the route.
    *
    * e.g. '/foo/:name/bar' with the map "name" -> "bob" would give you /foo/bob/bar"
    *
    * @param params the parts of the path used to satisfy the ':key' form elements
    * @return a Left of an error or a Right containing the uri parts
    */
  def resolve(params: Map[String, String] = Map.empty): Either[String, WebURI] = {
    import cats.syntax.either._

    val either: Either[String, List[ConstPart]] = uri.foldLeft(List[ConstPart]().asRight[String]) {
      case (Right(list), ParamPart(key)) =>
        params.get(key).map(value => ConstPart(value) :: list).toRight {
          s"The supplied parameters ${params.keySet.toList.sorted.mkString("[", ",", "]")} doesn't contain an entry for '$key'"
        }
      case (Right(list), const: ConstPart) => Right(const :: list)
    }

    either.map { parts: List[ConstPart] =>
      copy(uri = parts.reverse)
    }
  }
}

object WebURI {

  /**
    * represents part of a URI path
    */
  sealed trait Part

  object Part {
    private val ParamR = ":(.*)".r

    private def asPart(str: String): Part = {
      str match {
        case "*"       => ParamPart("*")
        case ParamR(n) => ParamPart(n)
        case n         => ConstPart(n)
      }
    }

    def apply(str: String): List[Part] = str.split("/", -1).map(_.trim).filterNot(_.isEmpty).map(asPart).toList
  }

  case class ConstPart(part: String) extends Part {
    override def toString: String = part
  }

  case class ParamPart(name: String) extends Part {
    override def toString: String = s":$name"
  }

  def get(uri: String): WebURI = WebURI(GET, Part(uri))

  def delete(uri: String): WebURI = WebURI(DELETE, Part(uri))

  def put(uri: String): WebURI = WebURI(PUT, Part(uri))

  def post(uri: String): WebURI = WebURI(POST, Part(uri))

  def head(uri: String): WebURI = WebURI(HEAD, Part(uri))

  def options(uri: String): WebURI = WebURI(OPTIONS, Part(uri))

  def apply(method: HttpMethod, uri: String): WebURI = new WebURI(method, Part(uri))

  def apply(method: HttpMethod, parts: Part*): WebURI = new WebURI(method, parts.toList)
}
