package riff

import _root_.monix.reactive.Observable
import riff.raft.messages.AddressedMessage
import riff.raft.node.NoOpResult.LogMessageResult
import riff.raft.node.{AddressedRequest, AddressedResponse, RaftNodeResult}

package object monix {

  def resultAsObservable[A](result: RaftNodeResult[A]): Observable[AddressedMessage[A]] = {
    result match {
      case AddressedRequest(requests) =>
        Observable.fromIterable(requests.map {
          case (node, msg) => AddressedMessage[A](node, msg)
        })
      case AddressedResponse(backTo, response) =>
        Observable.pure(AddressedMessage[A](backTo, response))
      case LogMessageResult(_) => Observable.empty[AddressedMessage[A]]
    }
  }
}
