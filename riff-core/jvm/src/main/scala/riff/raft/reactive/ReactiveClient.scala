package riff.raft.reactive
import org.reactivestreams.{Publisher, Subscriber}
import riff.raft.messages.AppendData
import riff.raft.node.RaftMessageHandler
import riff.raft.{AppendStatus, RaftClient}
import riff.reactive.ReactivePipe

import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

case class ReactiveClient[A: ClassTag](handler: RaftMessageHandler[A])(implicit executionContext: ExecutionContext)
    extends RaftClient[Publisher, A] {
  override def append(data: Array[A]): Publisher[AppendStatus] = {
    val pipe: ReactivePipe[AppendStatus, AppendStatus, Subscriber, Publisher] =
      ReactivePipe.single[AppendStatus](1000, 10, 100)
    val input: Subscriber[AppendStatus] = pipe.input
    val request = AppendData[A, Subscriber](input, data)
    handler.onMessage(request)
    pipe.output
  }
}
