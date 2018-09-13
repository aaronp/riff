package riff.raft.integration.simulator

import riff.raft.messages.{RaftRequest, RaftResponse}

/**
  * Events which can be put on the [[Timeline]]
  */
sealed trait TimelineType
case class SendTimeout(node: String)                                           extends TimelineType
case class ReceiveTimeout(node: String)                                        extends TimelineType
case class SendRequest(from: String, to: String, request: RaftRequest[String]) extends TimelineType
case class SendResponse(from: String, to: String, request: RaftResponse)       extends TimelineType
