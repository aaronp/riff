package riff.monix
import monix.reactive.Observable
import riff.raft.NodeId
import riff.raft.messages.{RequestOrResponse, TimerMessage}
import riff.raft.node.NoOpResult.LogMessageResult
import riff.raft.node.{AddressedRequest, AddressedResponse, RaftNodeResult}

sealed trait RaftStreamInput

object RaftStreamInput {

  def resultAsObservable[A](result: RaftNodeResult[A]): Observable[AddressedInput[A]] = {
    result match {
      case AddressedRequest(requests) =>
        Observable.fromIterable(requests.map {
          case (node, msg) => AddressedInput[A](node, msg)
        })
      case AddressedResponse(backTo, response) =>
        Observable.pure(AddressedInput[A](backTo, response))
      case LogMessageResult(_) => Observable.empty[AddressedInput[A]]
    }
  }
}
case class AddressedInput[A](id: NodeId, msg: RequestOrResponse[A]) extends RaftStreamInput
case class TimerInput(msg: TimerMessage) extends RaftStreamInput
