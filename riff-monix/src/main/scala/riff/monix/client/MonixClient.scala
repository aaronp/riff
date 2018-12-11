package riff.monix.client
import monix.execution.{Ack, Scheduler}
import monix.reactive.{Observable, Observer, Pipe}
import riff.monix.LowPriorityRiffMonixImplicits
import riff.raft.log.LogAppendResult
import riff.raft.messages.{AppendData, RaftMessage}
import riff.raft.{AppendStatus, RaftClient}

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
  * An implementation of [[RaftClient]] which will push incoming data into the input for the node (i.e. the inputSubscriber)
  *
  * @param inputSubscriber the input into a riff.raft.node.RaftMessageHandler
  * @param raftNodeLogResults the output of the log to which the inputSubscriber feeds in order to detect overwritten log entries
  * @param ev$1
  * @param sched
  * @tparam A
  */
case class MonixClient[A: ClassTag](inputSubscriber: Observer[RaftMessage[A]], raftNodeLogResults: Observable[LogAppendResult])(implicit sched: Scheduler)
    extends RaftClient[Observable, A] with LowPriorityRiffMonixImplicits {

  override def append(data: Array[A]): Observable[AppendStatus] = {

    // set up a pipe whose input can be used to subscribe to the RaftNode's feed,
    // and whose output we will return
    val (statusInput: Observer[AppendStatus], statusOutput: Observable[AppendStatus]) = Pipe.replay[AppendStatus].unicast

    // finally we can push an 'AppendData' message to the node
    val resFut: Future[Ack] = inputSubscriber.onNext(AppendData(statusInput, data))
    Observable.fromFuture(resFut).flatMap { _ => //
      statusOutput
    }
  }
}
