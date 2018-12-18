package riff.monix.log
import riff.raft.log.LogCoords

/**
  * Represents an observed status of the log
  *
  * @param lastAppended
  * @param lastCommitted
  */
case class LogStatus(lastAppended: LogCoords, lastCommitted: LogCoords)
