package riff.monix.log
import riff.raft.log.{LogAppendResult, LogCoords}

/**
  * The messages emitted by an ObservableLog
  */
sealed trait LogNotificationMessage

/**
  * published when entries are committed
  *
  * @param coords the committed coords
  */
final case class LogCommitted(coords: Seq[LogCoords]) extends LogNotificationMessage

/**
  * published when entries are committed
  *
  * @param coords the committed coords
  */
final case class LogAppended(appendResult: LogAppendResult) extends LogNotificationMessage
