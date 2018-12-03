package riff.monix
import monix.execution.Scheduler
import monix.reactive.subjects.{ConcurrentSubject, Var}
import monix.reactive.{Observable, Observer}
import riff.raft.log.{LogAppendResult, LogAppendSuccess, LogCoords}
import riff.raft.messages.{AddressedMessage, AppendEntriesResponse, RaftMessage}
import riff.raft.{AppendOccurredOnDisconnectedLeader, AppendStatus, NodeId}

object AppendState {

  private def apply(nodeId: NodeId, clusterSize: Int, leaderAppendResult: LogAppendResult): AppendState = {
    leaderAppendResult match {
      case logAppendSuccess: LogAppendSuccess =>
        val appendMap   = Map[NodeId, AppendEntriesResponse](nodeId -> AppendEntriesResponse.ok(logAppendSuccess.firstIndex.term, logAppendSuccess.lastIndex.index))
        val firstStatus = AppendStatus(logAppendSuccess, appendMap, logAppendSuccess.appendedCoords, clusterSize, clusterSize == 1)
        new FilteringState(firstStatus)
      case err: Exception => new ErrorState(err)
    }
  }

  private sealed trait AppendState {
    def update(appendResult: LogAppendResult, responseFrom: NodeId, appendResponse: AppendEntriesResponse): AppendState
    def onCommitted(coords : LogCoords) : AppendState
  }

  private case class ErrorState(err: Exception) extends AppendState {
    override def update(appendResult: LogAppendResult, responseFrom: NodeId, appendResponse: AppendEntriesResponse): AppendState = {
      this
    }
    override def onCommitted(coords: LogCoords) = this
  }

  private case class FilteringState(status: AppendStatus) extends AppendState {
    private val appendedCoords = status.appendedCoords
    override def update(appendResult: LogAppendResult, responseFrom: NodeId, appendResponse: AppendEntriesResponse): AppendState = {
      appendResult match {
      case someLogAppendResult: LogAppendSuccess =>
        val weAcceptedWhileDisconnected = someLogAppendResult.replacedLogCoords.exists(appendedCoords.contains)
        if (weAcceptedWhileDisconnected) {
          val err = new AppendOccurredOnDisconnectedLeader(status.leaderAppendResult, someLogAppendResult)
//          Observable.raiseError(err)
          ErrorState(err)
        } else {
          val nextStatus = status.withResult(responseFrom, appendResponse, someLogAppendResult.appendedCoords)
          FilteringState(nextStatus)
        }
      case someLogAppendError: Exception => ErrorState(someLogAppendError)
    }
  }
    override def onCommitted(coords: LogCoords): AppendState = {
      this
    }
  }

  def combine[A](nodeInput: Observable[RaftMessage[A]], appendResults: Observable[LogAppendResult], maxCapacity : Int): Observable[(LogAppendResult, (NodeId, AppendEntriesResponse))] = {
    /**
      * here's a hot, caching feed from the node input
      */
    val appendResponses: Observable[(NodeId, AppendEntriesResponse)] = nodeInput
      .collect {
        case AddressedMessage(from, resp @ AppendEntriesResponse(_, _, _)) => from -> resp
      }
      .cache(maxCapacity)

    appendResults.combineLatest(appendResponses)
  }

  /**
    * Create a feed of 'AppendStatus' based off the inputs coming into a node, the node's log results, and an initial LogAppendResult
    *
    * @param nodeInput used to populate the first [[AppendStatus]] event
    * @param clusterSize used to populate the first [[AppendStatus]] event
    * @param nodeInput the data coming into the node, which will be filtered on [[AppendEntriesResponse]] messages
    * @param appendResults
    * @param maxCapacity the buffer size of append responses to keep. Realistically this just needs to potentially cover a window between
    *                    this observer being created and a replay AppendStatus subscriber immediately subscribing
    * @param scheduler
    * @tparam A
    * @return and observer which should be given a single LogAppendResult from having tried an append entry, as well as an Observable of AppendStatus responses
    */
  def prepareAppendFeed[A](nodeId: NodeId,
                           clusterSize: Int,
                           nodeInput: Observable[RaftMessage[A]],
                           appendResults: Observable[LogAppendResult],
                           committedCoords: Observable[LogCoords],
                           maxCapacity: Int = 10)(
      implicit scheduler: Scheduler): (Observer[LogAppendResult], Observable[AppendStatus]) = {

    // get a feed of either log append updates or append responses
    val combinedObs: Observable[(LogAppendResult, (NodeId, AppendEntriesResponse))] = combine(nodeInput, appendResults, maxCapacity)

    val appendResultVar: ConcurrentSubject[LogAppendResult, LogAppendResult] = ConcurrentSubject.replay[LogAppendResult]
    val statusObservable: Observable[AppendStatus] = appendResultVar.flatMap { res =>

      val stateUpdates: Observable[AppendState] = combinedObs.scan(AppendState(nodeId, clusterSize, res)) {
        case (st8, (logResult, (fromNode, resp))) => st8.update(logResult, fromNode, resp)
      }

      val finalUpdates: Observable[AppendState] = stateUpdates.combineLatestMap(committedCoords)(_ onCommitted _)
      finalUpdates.flatMap {
        case ErrorState(err) => Observable.raiseError(err)
        case FilteringState(st8) => Observable(st8)
      }
    }
    appendResultVar -> statusObservable
  }
}
