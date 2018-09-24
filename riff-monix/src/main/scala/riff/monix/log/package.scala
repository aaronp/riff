package riff.monix
import riff.raft.log.{LogAppendSuccess, LogCoords}

package object log {

  type LogAppended = LogAppendSuccess

  type LogCommitted = Seq[LogCoords]
}
