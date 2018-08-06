package riff.raft.append
import riff.raft.log.LogCoords
import simulacrum.typeclass

/**
  * The rules for appending are:
  *
  * 1) The match index will be the previous log index + 1 if the previous log index ([[LogCoords]]) supplied
  * has the same term for the given index as this node's (which is determined by a call to 'logMatches')
  *
  * 2)
  * === heartbeat ===
  * If the entries are empty, then this is a heartbeat message
  *
  * We are successful if the term of the request is greater than our term, then we should immediately become
  * a follower w/ the new term and return success
  *
  * @tparam A the raft node type
  * @tparam T log entry data type
  */
@typeclass trait HasInfoForAppend[A, T] {

  def currentTerm(raftNode: A): Int

  /** @param previous the log index and term to check
    * @return true if our commit log has the same term at the given index
    */
  def logMatches(previous: LogCoords): Boolean

  def onAppend(raftNode: A, request: AppendEntriesData[T]): AppendEntriesReply = {
    val ourTerm = currentTerm(raftNode)

    val matchIndex = if (logMatches(request.previous)) {
      request.previous.index + 1
    } else {
      0
    }

    val success = request.term >= ourTerm
    AppendEntriesReply(request.term, success, matchIndex)
  }
}
