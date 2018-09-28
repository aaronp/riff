package riff.raft.timer
import riff.raft.NodeId

import scala.collection.mutable.ListBuffer

class LoggedInvocationTimer extends RaftTimer {
  override type CancelT = String
  private val receiveCalls = ListBuffer[(NodeId, Option[String])]()
  private val sendCalls    = ListBuffer[(NodeId, Option[String])]()
  private val cancelCalls  = ListBuffer[String]()

  def resetReceiveHeartbeatCalls() = receiveCalls.toList
  def resetSendHeartbeatCalls()    = sendCalls.toList
  def cancelHeartbeatCall()        = cancelCalls.toList

  override def resetReceiveHeartbeatTimeout(raftNode: NodeId, previous: Option[String]): String = {
    receiveCalls += (raftNode -> previous)
    "" + receiveCalls.size
  }
  override def resetSendHeartbeatTimeout(raftNode: NodeId, previous: Option[String]): String = {
    sendCalls += (raftNode -> previous)
    "" + sendCalls.size

  }
  override def cancelTimeout(c: String): Unit = {
    cancelCalls += c
  }
}
