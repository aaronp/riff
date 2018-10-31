package riff.raft.reactive
import org.reactivestreams.{Publisher, Subscriber}
import riff.raft.messages.{AppendData, RaftMessage}
import riff.raft.node.RaftNodeResult
import riff.raft.{AppendStatus, RaftClient}
import riff.reactive.{AsPublisher, ReactivePipe}

import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

case class ReactiveClient[A: ClassTag, Pub[_]: AsPublisher](pipe: ReactivePipe[RaftMessage[A], RaftNodeResult[A], Subscriber, Publisher])(
  implicit executionContext: ExecutionContext)
    extends RaftClient[Publisher, A] {

  override def append(data: Array[A]): Publisher[AppendStatus] = {
    val newPipe = ReactivePipe.single[AppendStatus](1000, 10, 100)
    pipe.inputSubscriber.onNext(AppendData(newPipe.input, data))
    newPipe.output
  }
}
