package riff.monix
import monix.execution.Scheduler
import monix.reactive.{Observable, Observer, Pipe}
import riff.raft.messages.{AppendData, RaftMessage}
import riff.raft.{AppendStatus, RaftClient}

import scala.reflect.ClassTag

case class RiffMonixClient[A: ClassTag](inputSubscriber: Observer[RaftMessage[A]])(implicit sched: Scheduler) extends RaftClient[Observable, A] {

  override def append(data: Array[A]): Observable[AppendStatus] = {
    val (statusInput, statusOutput) = Pipe.replay[AppendStatus].unicast
    
    inputSubscriber.onNext(AppendData(statusInput.toReactive, data))
    statusOutput
  }

}
