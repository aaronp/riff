package riff.monix
import monix.execution.Scheduler
import monix.reactive.{Observable, Observer, Pipe}
import riff.raft.log.LogAppendResult
import riff.raft.messages.{AppendData, RaftMessage}
import riff.raft.{AppendStatus, RaftClient}

import scala.reflect.ClassTag

case class RiffMonixClient[A: ClassTag](inputSubscriber: Observer[RaftMessage[A]], raftNodeLogResults: Observable[LogAppendResult])(implicit sched: Scheduler)
    extends RaftClient[Observable, A] {

  override def append(data: Array[A]): Observable[AppendStatus] = {
    val (statusInput, statusOutput) = Pipe.replay[AppendStatus].unicast

    import LowPriorityRiffMonixImplicits._

    /**
      * this is actually somewhat complicated.
      *
      * We need to avoid race-conditions, but also don't want to over-complicate our model (or impose unnecessary detail/complexity on our inputs),
      * especially as we've already compromised by exposing a responseSubscriber in our AppendData message.
      *
      * So the case is this:
      *
      * We have a 'pipe' representing the inputs/outputs of a node, decoupled from the 'handler' which maps those inputs into outputs.
      *
      * At this point, we want to be able to just append some data of type 'T' to a node (which should and will fail if that node
      * isn't the leader -- and that's okay, as it's a lot easier to provide retry logic to for the leader, especially as we have
      * specific exceptions like [[riff.raft.log.NotTheLeaderException]] to help us redirect).
      *
      * But, just having a 'T' means we don't have a log index yet. And so, essentially, we have to:
      *
      * 1) start listening to this node's inputs in order to intercept the AppendResponse messages BEFORE we append
      * to avoid that race condition
      *
      * 2) listen to the node's logResults in order to deal with more complex scenarios, like when we THINK we're the leader,
      * but realize later that we're stale and another election/leader has occurred (and we've potentially been accepting append
      * requests while we still thought we were the leader)
      *
      *
      * In order to support scenario #2, we'll need to:
      * - read at least the first AppendStatus message (which should be near instant, as it comes from the leader itself)
      * - given the data from above, we know the firstIndex/lastIndex range we've appended while we thought we were the leader
      * - for every message received from the raftNodeLogResults, we need to ensure the 'replacedIndices' don't include our entries
      *
      *
      * NOTE: You could argue this complexity is overkill, and downstream clients should just listen to the event streams they're
      * interested in. It's also typically necessary to have some 'timeout' concept, as delivery is not guaranteed, and so the listener
      * to an 'append' request is not guaranteed to get all responses anyway (e.g. a failed node in the cluster may not be responsive,
      * or in fact EVER come back up), so clients should be defensive about that.
      *
      */
    inputSubscriber.onNext(AppendData(statusInput, data))
    statusOutput
  }

}
