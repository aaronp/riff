package riff.monix.client
import cats.kernel.Eq
import monix.execution.{Ack, Scheduler}
import monix.reactive.observables.ConnectableObservable
import monix.reactive.subjects.ConcurrentSubject
import monix.reactive.{Observable, Observer, Pipe}
import riff.raft.client._
import riff.raft.log.{LogAppendResult, LogCoords}
import riff.raft.messages.{AddressedMessage, AppendEntriesResponse, RaftMessage}
import riff.raft.{AppendStatus, NodeId}

import scala.concurrent.Future

/**
  * Converts the observable inputs to a [[riff.raft.node.RaftNode]] and its [[riff.monix.log.ObservableLog]] in order to
  * provide an Observable[AppendStatus] which can be used to observe the result of an append action from a [[riff.raft.RaftClient]]
  */
object AppendStatusObservable {

  /**
    * Create a feed of 'AppendStatus' based off the inputs coming into a node, the node's log results, and an initial LogAppendResult
    *
    * @param nodeInput used to populate the first [[AppendStatus]] event
    * @param clusterSize used to populate the first [[AppendStatus]] event
    * @param nodeInput the data coming into the node, which will be filtered on [[AppendEntriesResponse]] messages
    * @param appendResults
    * @param maxCapacity the buffer size of append responses to keep. In practice this should be < 10 elements, so 'maxCapacity' is defaulted quite low.
    *       If a client appends a LOT of data though, there could be a lot of ACKS and commits, so this should be driven by a function of the size of the cluster, batch size, and number of elements appended
    * @param scheduler
    * @tparam A
    * @return and observer which should be given a single LogAppendResult from having tried an append entry, as well as an Observable of AppendStatus responses
    */
  def apply[A](nodeId: NodeId,
               clusterSize: Int,
               nodeInput: Observable[RaftMessage[A]],
               appendResults: Observable[LogAppendResult],
               committedCoordsInput: Observable[LogCoords],
               maxCapacity: Int = 50)(implicit scheduler: Scheduler): (Observer[LogAppendResult], Observable[AppendStatus]) = {
    val stateInput = ConcurrentSubject.publish[StateUpdateMsg]

    // subscribe to our concurrent subject
    val updateMessages: Observable[StateUpdateMsg] = {
      val (pipeIn, pipeOut) = Pipe.publishToOne[StateUpdateMsg].unicast
      stateInput.subscribe(pipeIn)

      val responseMessages = nodeInput.collect {
        case AddressedMessage(from, resp @ AppendEntriesResponse(_, _, _)) => StateUpdateMsg.responseFromNode(from, resp)
      }

      responseMessages.subscribe(stateInput)
      appendResults.map(StateUpdateMsg.logAppend).subscribe(stateInput)
      committedCoordsInput.map(StateUpdateMsg.logCommit).subscribe(stateInput)

      pipeOut
    }

    // the observer which will be given the single log result from the data which has been appended
    val appendLogResultObserver = new Observer[LogAppendResult] {
      override def onNext(elem: LogAppendResult): Future[Ack] = stateInput.onNext(StateUpdateMsg.initialAppend(elem))
      override def onError(ex: Throwable): Unit               = stateInput.onError(ex)
      override def onComplete(): Unit                         = stateInput.onComplete()
    }

    val result: Observable[AppendStatus] = {
      val outputFeed = updateMessages.scan(SingleAppendFSM(nodeId, clusterSize)) {
        case (st8, msg) => st8.update(msg)
      }

      // we use 'None' to signal a "poison pill" to determine when to stop
      val statusUpdates: Observable[AppendStatus] = outputFeed
        .flatMap {
          case InitialState(_, _)                               => Observable.empty
          case ErrorState(err)                                  => Observable.raiseError(err)
          case EarlyTerminationOnErrorState(status)             => Observable(Option(status), None)
          case FilteringState(_, None)                          => Observable.empty
          case FilteringState(_, Some(next)) if next.isComplete => Observable(Option(next), None)
          case FilteringState(_, Some(next))                    => Observable(Option(next))
        }
        .takeWhile(_.isDefined)
        .map(_.get)

      // allow replay of the status updates
      if (maxCapacity > 0) {
        implicit val statusEq                           = Eq.fromUniversalEquals[AppendStatus]
        val replay: ConnectableObservable[AppendStatus] = statusUpdates.distinctUntilChanged.replay(maxCapacity)
        replay.connect()
        replay
      } else {
        statusUpdates
      }
    }

    appendLogResultObserver -> result

  }
}
