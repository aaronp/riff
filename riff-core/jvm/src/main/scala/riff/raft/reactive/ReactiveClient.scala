package riff.raft.reactive
import org.reactivestreams.{Publisher, Subscriber}
import riff.raft.messages.{AppendData, RaftMessage}
import riff.raft.{AppendStatus, RaftClient}
import riff.reactive.ReactivePipe

import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

case class ReactiveClient[A: ClassTag](inputSubscriber: Subscriber[RaftMessage[A]])(implicit executionContext: ExecutionContext) extends RaftClient[Publisher, A] {
  override def append(data: Array[A]): Publisher[AppendStatus] = {
    val newPipe = ReactivePipe.single[AppendStatus](1000, 10, 100)
    inputSubscriber.onNext(AppendData(newPipe.input, data))
    newPipe.output
  }
}
