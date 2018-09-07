package riff.raft.timer

class Timers[NodeKey]()(implicit val timer: RaftTimer[NodeKey]) {
  class cancelableMap(doReset: (NodeKey, Option[timer.CancelT]) => timer.CancelT) {
    private var cancelableTaskByKey = Map[NodeKey, timer.CancelT]()
    def cancel(node: NodeKey) = {
      cancelableTaskByKey.get(node).foreach { c =>
        timer.cancelTimeout(c)
        cancelableTaskByKey = cancelableTaskByKey.updated(node, c)
      }
    }

    def reset(node: NodeKey): timer.CancelT = {
      val cancelable = cancelableTaskByKey.get(node)
      val c          = doReset(node, cancelable)
      cancelableTaskByKey = cancelableTaskByKey.updated(node, c)
      c
    }
  }

  object receiveHeartbeat
      extends cancelableMap({
        case (node: NodeKey, cancelable: Option[timer.CancelT]) => timer.resetReceiveHeartbeatTimeout(node, cancelable)
      })

  object sendHeartbeat
      extends cancelableMap({
        case (node: NodeKey, cancelable: Option[timer.CancelT]) => timer.resetSendHeartbeatTimeout(node, cancelable)
      })
}
