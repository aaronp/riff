package riff.reactive
import riff.raft.messages.{ReceiveHeartbeatTimeout, SendHeartbeatTimeout, TimerMessage}
import riff.raft.timer.TimerCallback

import scala.concurrent.ExecutionContext

/**
  * An implementation of a [[TimerCallback]] which publishes TimerMessages
  *
  * @param ctxt the execution context used for each subscriber to consumer its messages
  */
class ReactiveTimerCallback private (override val maxQueueSize: Int = 1000)(
  override implicit val ctxt: ExecutionContext)
    extends TimerCallback[TimerMessage] with AsyncPublisher[TimerMessage] {

  def onTimeout(msg: TimerMessage): TimerMessage = {
    enqueueMessage(msg)
    msg
  }

  override def onSendHeartbeatTimeout(): TimerMessage = {
    onTimeout(SendHeartbeatTimeout)
  }
  override def onReceiveHeartbeatTimeout(): TimerMessage = {
    onTimeout(ReceiveHeartbeatTimeout)
  }

}

object ReactiveTimerCallback {

  def apply()(implicit ctxt: ExecutionContext): ReactiveTimerCallback = {
    new ReactiveTimerCallback
  }
}
