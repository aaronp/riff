package riff.raft.timer
import scala.collection.mutable.ListBuffer

class LoggedInvocationTimer[NodeKey] extends RaftTimer[NodeKey] {
  override type CancelT = String
  private val receiveCalls = ListBuffer[(NodeKey, Option[String])]()
  private val sendCalls    = ListBuffer[(NodeKey, Option[String])]()
  private val cancelCalls  = ListBuffer[String]()

  def resetReceiveHeartbeatCalls() = receiveCalls.toList
  def resetSendHeartbeatCalls()    = sendCalls.toList
  def cancelHeartbeatCall()        = cancelCalls.toList

  override def resetReceiveHeartbeatTimeout(raftNode: NodeKey, previous: Option[String]): String = {
    receiveCalls += (raftNode -> previous)
    "" + receiveCalls.size
  }
  override def resetSendHeartbeatTimeout(raftNode: NodeKey, previous: Option[String]): String = {
    sendCalls += (raftNode -> previous)
    "" + sendCalls.size

  }
  override def cancelTimeout(c: String): Unit = {
    cancelCalls += c
  }
}
