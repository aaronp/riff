package riff.raft.messages

import riff.raft.log.{LogCoords, LogEntry}
import riff.raft.{LogIndex, Term}

/**
  * Represents all the inputs into a raft node
  *
  * @tparam A
  */
sealed trait RaftMessage[+NodeKey, +A]

sealed trait RequestOrResponse[+NodeKey, +A] extends RaftMessage[NodeKey, A]

sealed trait TimerMessage extends RaftMessage[Nothing, Nothing]

/** Marks a timeout of not hearing from a leader
  */
case object ReceiveHeartbeatTimeout extends TimerMessage

/** Marks a timeout for a leader indicating it should sent a heartbeat to the given follower
  */
case object SendHeartbeatTimeout extends TimerMessage

/**
  * RaftRequest
  *
  * @tparam A
  */
sealed trait RaftRequest[+A] extends RequestOrResponse[Nothing, A]

final case class AppendEntries[A](previous: LogCoords, term: Term, commitIndex: LogIndex, entries: Array[LogEntry[A]] = Array.empty[LogEntry[A]])
    extends RaftRequest[A] {

  def appendIndex = previous.index + 1

  override def equals(obj: Any): Boolean = {
    obj match {
      case AppendEntries(`previous`, `term`, `commitIndex`, otherEntries) if otherEntries.length == entries.length =>
        entries.zip(otherEntries).forall {
          case (a, b) => a == b
        }
      case _ => false
    }
  }
  override def toString = {
    val entrySize = entries.length
    val entryStr = if (entrySize < 5) {
      entries.mkString(",")
    } else {
      entries.take(4).mkString("", ",", ",...")
    }
    s"""AppendEntries(previous=$previous, term=$term, commitIndex=$commitIndex}, ${entrySize} entries=[$entryStr])"""
  }
}

final case class RequestVote(term: Term, logState: LogCoords) extends RaftRequest[Nothing] {
  def lastLogIndex: LogIndex = logState.index
  def lastLogTerm: LogIndex  = logState.term
}

/**
  * Raft Response
  */
sealed trait RaftResponse extends RequestOrResponse[Nothing, Nothing]

final case class RequestVoteResponse(term: Term, granted: Boolean) extends RaftResponse

final case class AppendEntriesResponse private (term: Term, success: Boolean, matchIndex: Int) extends RaftResponse {
  require(success || matchIndex == 0, s"Match index '${matchIndex}' should instead be 0 if success is false")
  require(matchIndex >= 0, s"Match index '${matchIndex}' should never be negative")
}
object AppendEntriesResponse {
  def fail(term: Term)                = AppendEntriesResponse(term, false, 0)
  def ok(term: Term, matchIndex: Int) = AppendEntriesResponse(term, true, matchIndex)
}
