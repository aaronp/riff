package riff
import riff.raft.log.LogCoords

final case class AppendResult(logCoods: LogCoords, appended: Map[String, Boolean], clusterSize: Int)
