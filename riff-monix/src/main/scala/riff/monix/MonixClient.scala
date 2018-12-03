package riff.monix
import cats.kernel.Eq
import monix.execution.{Ack, Scheduler}
import monix.reactive.{Observable, Observer, Pipe}
import riff.raft.log.{LogAppendResult, LogAppendSuccess, LogCoords}
import riff.raft.messages.{AppendData, RaftMessage}
import riff.raft.{AppendOccurredOnDisconnectedLeader, AppendStatus, RaftClient}

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
  override def append(data: Array[A]): Observable[AppendStatus] = {

    // set up a pipe whose input can be used to subscribe to the RaftNode's feed,
    // and whose output we will return
    val (statusInput: Observer[AppendStatus], statusOutput: Observable[AppendStatus]) = Pipe.replay[AppendStatus].unicast

    // finally we can push an 'AppendData' message to the node
    val resFut: Future[Ack] = inputSubscriber.onNext(AppendData(statusInput, data))
    Observable.fromFuture(resFut).dump(data.mkString("client onNext ack [", ",", "]")).flatMap { _ => //
      statusOutput
    }
  }
}

object MonixClient {

  def createAppendStatusFeed[A](data: Array[A], statusOutput: Observable[AppendStatus], raftNodeLogResults: Observable[LogAppendResult])(implicit sched: Scheduler) = {

    // we need to be able to get the first AppendStatus which will be from the 'inputSubscriber's node
    // if it is in fact the leader (if it isn't all of this will be for naught, as the whole observable will
    // just be failed by the node).
    val head: Observable[AppendStatus] = statusOutput.take(1)

    // once we have the first status then we'll know the coords of the append.
    // if the log ever reports that those entries are replace then we'll fail this stream
    val tail = statusOutput.drop(1)

    head.flatMap {
      case firstStatus: AppendStatus if firstStatus.clusterSize > 1 =>
        val appendedCoords: Set[LogCoords] = firstStatus.appendedCoords
        val logAppends                     = raftNodeLogResults.dump(data.mkString("Monix Client LOG input [", ",", "]"))

        val combinedObs: Observable[Observable[AppendStatus]] = tail.combineLatestMap(logAppends) {
          case (nextStatus, someLogAppendResult: LogAppendSuccess) =>
            val weAcceptedWhileDisconnected = someLogAppendResult.replacedLogCoords.exists(appendedCoords.contains)
            if (weAcceptedWhileDisconnected) {
              val err = new AppendOccurredOnDisconnectedLeader(firstStatus.leaderAppendResult, someLogAppendResult)
              Observable.raiseError(err)
            } else {
              Observable(nextStatus)
            }
          case (_, someLogAppendError: Exception) => Observable.raiseError(someLogAppendError)
        }

        val combined    = combinedObs.flatten.dump(data.mkString("Monix Client combined [", ",", "]"))
        implicit val eq = Eq.instance[AppendStatus](_ == _)
        val res =
          (firstStatus +: combined).dump(data.mkString("Monix Client first + combined [", ",", "]")).distinctUntilChanged.dump(data.mkString("Monix Client distinct [", ",", "]"))

        LowPriorityRiffMonixImplicits.observableAsPublisher(sched).takeWhileIncludeLast(res)(!_.isComplete)

      case firstStatus: AppendStatus =>
        Observable(firstStatus).dump(data.mkString("first status [", ",", "]"))

    }
  }
}
