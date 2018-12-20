package riff.rest

class Route[F[_]](uri : WebURI, handler : RestRequestHandler[F])
