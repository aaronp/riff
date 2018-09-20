package riff.raft.integration.simulator

import riff.raft.log.{LogCoords, LogEntry}
import riff.raft.messages._

/**
  * Events which can be put on the [[Timeline]]
  */
sealed trait TimelineType {
  def asAssertion() : String = {
    def coordsAsAssertion(coords : LogCoords) = {
      s"LogCoords(${coords.term}, ${coords.index})"
    }
    this match {
      case SendTimeout(name) => s"SendTimeout($name)"
      case ReceiveTimeout(name) => s"ReceiveTimeout($name)"
      case SendRequest(from, to, AppendEntries(previous, term, commit, entries)) =>
        val entryValues = entries.map{
          case LogEntry(term, data) => s"($term,$data)"
        }.mkString("[", ",", "]")
        s"SendRequest($from, $to, AppendEntries(previous=${coordsAsAssertion(previous)}, term=$term, commit=$commit, ${entryValues}))"
      case SendRequest(from, to, RequestVote(term, coords)) =>
        s"SendRequest($from, $to, RequestVote(term=$term, coords=${coordsAsAssertion(coords)}))"
      case SendResponse(from, to, RequestVoteResponse(term, granted)) =>
        s"SendResponse($from, $to, RequestVoteResponse(term=$term, granted=$granted))"
      case SendResponse(from, to, AppendEntriesResponse(term, success, matchIndex)) =>
        s"SendResponse($from, $to, AppendEntriesResponse(term=$term, success=$success, matchIndex=$matchIndex))"
    }
  }
}
case class SendTimeout(node: String)                                           extends TimelineType
case class ReceiveTimeout(node: String)                                        extends TimelineType
case class SendRequest(from: String, to: String, request: RaftRequest[String]) extends TimelineType
case class SendResponse(from: String, to: String, request: RaftResponse)       extends TimelineType
