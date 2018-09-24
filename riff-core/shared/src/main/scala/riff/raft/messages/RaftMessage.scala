package riff.raft.messages

import riff.raft.log.{LogCoords, LogEntry}
import riff.raft.{LogIndex, NodeId, Term}

import scala.reflect.ClassTag

/** Represents the possible input types into a [[riff.raft.node.RaftNode]]
  *
  * Encapsulating all inputs w/ a single type makes the [[riff.raft.node.RaftNode]] easy to drive via a data stream,
  * put behind an actor, invoke from an unmarshalled POST request, etc.
  *
  * @tparam A the log entry type. This type is added here (as opposed to just on the [[AddressedMessage]] as an aid for circe to be able to provide generic encoder/decoders
  */
sealed trait RaftMessage[+A]

/** @param from represents the source of the message
  * @param message the message
  * @tparam A
  */
final case class AddressedMessage[+A](from: NodeId, message: RequestOrResponse[A]) extends RaftMessage[A]

/** Represents input from a client of the cluster
  * @param values the values to append
  * @tparam A the log entry type.
  */
final case class AppendData[A:ClassTag](values : Array[A]) extends RaftMessage[A] {
  override def toString() = {
    if (values.size > 5) {
      s"AppendData(${values.take(5).mkString(s"${values.size} : [",",","...]")})"
    } else {
      s"AppendData(${values.mkString("[",",","]")})"
    }
  }
  override def hashCode(): LogIndex = {
    values.foldLeft(17) {
      case (hash, next) => hash + (next.hashCode() * 7)
    }
  }

  override def equals(other : Any) = {
    other match {
      case AppendData(otherValues) if values.length == otherValues.length =>
        values.zip(otherValues).forall {
          case (a,b) => a == b
        }
      case _ => false
    }
  }
}
object AppendData {
  def apply[A: ClassTag](first : A, theRest : A*) = new AppendData(first +: theRest.toArray)
}

sealed trait TimerMessage extends RaftMessage[Nothing]

sealed trait RequestOrResponse[+A] {
  def from(name: NodeId): AddressedMessage[A] = AddressedMessage[A](name, this)
}

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
sealed trait RaftRequest[+A] extends RequestOrResponse[A]

final case class AppendEntries[A](
  previous: LogCoords,
  term: Term,
  commitIndex: LogIndex,
  entries: Array[LogEntry[A]] = Array.empty[LogEntry[A]])
    extends RaftRequest[A] {

  def appendIndex = previous.index + 1

  override def hashCode(): LogIndex = {
    val hash = entries.foldLeft(17) {
      case (hash, next) => hash + (next.hashCode() * 7)
    }
    hash + (previous.hashCode() << 1) + (term.hashCode() << 2) + (commitIndex.hashCode() << 3)
  }
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
  def lastLogTerm: LogIndex = logState.term
}

/**
  * Raft Response
  */
sealed trait RaftResponse extends RequestOrResponse[Nothing]

final case class RequestVoteResponse(term: Term, granted: Boolean) extends RaftResponse

final case class AppendEntriesResponse private (term: Term, success: Boolean, matchIndex: Int) extends RaftResponse {
  require(success || matchIndex == 0, s"Match index '${matchIndex}' should instead be 0 if success is false")
  require(matchIndex >= 0, s"Match index '${matchIndex}' should never be negative")
}

object AppendEntriesResponse {
  def fail(term: Term) = AppendEntriesResponse(term, false, 0)
  def ok(term: Term, matchIndex: Int) = AppendEntriesResponse(term, true, matchIndex)
}
