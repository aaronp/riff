package riff.rest

trait RestRequestHandler[F[_]] {
  def onRequest(request : RestRequest) : F[RestResponse]
}
