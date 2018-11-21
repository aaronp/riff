package riff.raft.log

import riff.raft.messages.{AppendEntries, AppendEntriesResponse}
import riff.raft.{LogIndex, Term}

import scala.reflect.ClassTag

/**
  * A base type for the raft log.
  *
  * the [[RaftLogOps#onCommit]] is the primary, important bit which binds the commit of the log entry to your application.
  * That is the point where the entry is applied to your state-machine.
  *
  *
  * The 'RaftLogOps' is declared from which RaftLog extends, as the JVM-specific implemenation uses the 'RaftLog' companion
  * object with its dependency on the 'ToBytes'/'FromBytes' typeclasses, which as I write this, I
  * realize how silly a reason that perhaps is to split it up.
  *
  * At any rate, the end-client consumers of this trait won't know, as both the JVM and Javascript variants expose a RaftLog, so it shouldn't really
  * impact anybody, other than to give 'em pause as they think "why this extra step in the class hierarchy?"
  *
  * @tparam A the type stored in the log
  */
trait RaftLogOps[A] { self: RaftLog[A] =>

  /** Append all the data entries
    *
    * @param fromIndex the index of the first data element
    * @param data the entries (term + data) to append
    * @return the result from appending the data
    */
  def appendAll(fromIndex: LogIndex, data: Array[LogEntry[A]]): LogAppendResult

  /**
    * Append the given log entry w/ the given coords (term and index)
    *
    * @param coords the log term and index against which this log should be appended
    * @param data
    * @return the append result
    */
  def append(coords: LogCoords, data: A, theRest: A*)(implicit classTag: ClassTag[A]): LogAppendResult = {
    val firstEntry = LogEntry(coords.term, data)
    val arr = if (theRest.isEmpty) {
      Array(firstEntry)
    } else {
      firstEntry +: theRest.map(v => LogEntry(coords.term, v)).toArray
    }
    appendAll(coords.index, arr)
  }

  def latestCommit(): LogIndex

  /** @param index
    * @return the log term for the latest index
    */
  def termForIndex(index: LogIndex): Option[Term]

  def latestAppended(): LogCoords

  /** @param entry the entry to check
    * @return true if there is a log entry for the given coords
    */
  def contains(entry: LogCoords): Boolean = termForIndex(entry.index).exists(_ == entry.term)

  /**
    * Commit all the entries up to the given index.
    *
    * It is the responsibility of the node to determine whether this should be called, knowing
    * that this log is safe to commit
    *
    * @param index the index to commit
    * @return the coordinates of all entries committed
    */
  def commit(index: LogIndex): Seq[LogCoords]

  /** @param index the index for which the log entry should be retrieved
    * @return the [[LogEntry]] at the given index, if it exists
    */
  def entryForIndex(index: LogIndex): Option[LogEntry[A]]

  /**
    * @return the latest appended entry LogState (term and index)
    */
  def logState(): LogState = {
    val LogCoords(term, index) = latestAppended()
    LogState(latestCommit(), term, index)
  }

  final def coordsForIndex(index: LogIndex): Option[LogCoords] = termForIndex(index).map(LogCoords(_, index))

  /** @return a caching version of this log -- an optimisation for keeping in-memory the latestCommit and latestAppended
    */
  def cached(): CachingLog[A] = {
    this match {
      case c: CachingLog[A] => c
      case other            => new CachingLog[A](other)
    }
  }

  /** This function may server as the main domain logic hook for of the Raft cluster -- it applies the provided function
    * to the LogEntry[A] once the entry is committed. It is up to the application what to do with
    * the side-effectful function.
    *
    * You can chain these methods to do multiple things, but notice that this method returns a NEW
    * log, and so all of this should be done prior to creating the RaftNode which will handle the
    * cluster events.
    *
    * For example:
    *
    * {{{
    *
    *   def businessLogicOnCommit(json : Json) = ...
    *
    *   val raftNode : RaftNode[String, Json] = {
    *      val dataDir : Path = ...
    *      val node = RaftNode[Json](dataDir)
    *      node.withLog(node.log.onCommit { entry =>
    *         businessLogicOnCommit(entry.data)
    *     }
    *   }
    *
    *   // put the raft node behind some REST routes of some framework....
    *   val routes = createWebRouts(raftNode)
    *   Http.bind(8080, routes)
    *
    * }}}
    *
    * @return a log which applies the given function when the given log entry is committed.
    */
  def onCommit(applyToStateMachine: LogEntry[A] => Unit): StateMachineLog[A] = StateMachineLog(this)(applyToStateMachine)

  /**
    * @param firstIndex the matching log index, WHICH IS ONE-BASED
    * @param max the maximum entries to return
    * @return the log entries from the given match index
    */
  def entriesFrom(firstIndex: LogIndex, max: Int = Int.MaxValue): Array[LogEntry[A]] = {
    val mostRecentAppend = latestAppended()
    val opts             = (firstIndex.max(1) to mostRecentAppend.index).view.map(entryForIndex)
    opts
      .takeWhile(_.isDefined)
      .take(max)
      .collect {
        case Some(x) => x
      }
      .toArray
  }

  protected def containsIndex(index: LogCoords) = {
    entryForIndex(index.index).exists(_.term == index.term)
  }

  /**
    * append the entry to the log.
    *
    * If the log entry matches _any_ of this raft node's log entries then this is successful, and the match
    * index will be set to the request previous index + 1.
    *
    * @param raftNode
    * @param request
    * @return the AppendEntriesResponse
    */
  def onAppend(currentTerm: Term, request: AppendEntries[A]): AppendEntriesResponse = {
    val latest = latestAppended()

    //
    // we're successful if
    // 1) common case - it's another append on top of our latest index
    // 2) we're a follower whose fallen behind. If that's the case, we need to
    //    a) check the previous matches our log
    //    -- or --
    //    b) the previous is the first index
    //
      val matchedPrevious = latest == request.previous || containsIndex(request.previous)

    val success = {
      // we could be in the situation where a leader appended an unreplicated entry and subsequently became a follower
      matchedPrevious || (request.previous.index == 0)
    }

    val matchIndex: LogIndex = if (success) {
      if (request.entries.nonEmpty) {
        val latestCommitIndex = latestCommit()
        require(latestCommitIndex <= request.previous.index, s"Attempt to append at ${request.previous} when the latest commit is $latestCommitIndex")
        val logAppendResult: LogAppendResult = appendAll(request.appendIndex, request.entries)

        logAppendResult match {
          case LogAppendSuccess(_, lastIndex, _) => lastIndex.index
          case _                                 => 0
        }
      } else {
        if (request.previous.index == 0) {
          0
        } else {
          latest.index
        }
      }
    } else {
      0
    }

    // double-check - should we fail w/ the node's term or the latest log entry term?
    if (success) {
      AppendEntriesResponse.ok(request.term, matchIndex)
    } else {
      AppendEntriesResponse.fail(currentTerm)
    }
  }
}
