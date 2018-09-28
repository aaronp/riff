package riff.raft.timer
import riff.raft.NodeId

class Timers()(implicit val timer: RaftTimer) {
  class cancelableMap(doReset: (NodeId, Option[timer.CancelT]) => timer.CancelT) {
    private var cancelableTaskByKey = Map[NodeId, timer.CancelT]()
    def cancel(node: NodeId) = {
      cancelableTaskByKey.get(node).foreach { c =>
        timer.cancelTimeout(c)
        cancelableTaskByKey = cancelableTaskByKey.updated(node, c)
      }
    }

    def reset(node: NodeId): timer.CancelT = {
      val cancelable = cancelableTaskByKey.get(node)
      val c          = doReset(node, cancelable)
      cancelableTaskByKey = cancelableTaskByKey.updated(node, c)
      c
    }
  }

  object receiveHeartbeat
      extends cancelableMap({
        case (node: NodeId, cancelable: Option[timer.CancelT]) => timer.resetReceiveHeartbeatTimeout(node, cancelable)
      })

  object sendHeartbeat
      extends cancelableMap({
        case (node: NodeId, cancelable: Option[timer.CancelT]) => timer.resetSendHeartbeatTimeout(node, cancelable)
      })
}
