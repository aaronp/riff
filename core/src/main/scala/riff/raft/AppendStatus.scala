package riff.raft
import riff.raft.log.LogCoords

/**
  * Represents the current state of the cluster following an append
  *
  * @param logCoods the coords of the entry appended
  * @param appended a map of the cluster ids to a flag indicating whether or not the node has appended the entry
  * @param committed a flag to indicate whether the entry has been committed on the leader
  * @param clusterSize the size of the cluster
  */
final case class AppendStatus(logCoods: LogCoords, appended: Map[String, Boolean], committed: Boolean, clusterSize: Int)
