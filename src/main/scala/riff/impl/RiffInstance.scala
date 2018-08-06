package riff.impl
//
//class RiffInstance[A](send: RaftClusterClient[Observable], commitLog: RaftLog[A], timer: RaftTimer)(
//    implicit sched: Scheduler)
//    extends Riff[A]
//    with StrictLogging {

//  private val (membershipObserver, membershipObservable) = Pipe.publish[ClusterEvent].multicast
//
//  private var sendHeartbeatTimeoutOpt: Option[timer.C]     = None
//  private var receivedHeartbeatTimeoutOpt: Option[timer.C] = None
//
//  override def membership: Publisher[ClusterEvent] = {
//    stateRef.transformAndExtract[Publisher[ClusterEvent]] { state =>
//      val adds: Observable[NodeAdded]   = Observable.fromIterable(state.node.clusterNodes.map(ClusterEvent.nodeAdded))
//      val all: Observable[ClusterEvent] = adds ++ membershipObservable
//      all.toReactivePublisher[ClusterEvent] -> state
//    }
//  }
//
//  override def append(data: A): Publisher[AppendResult] = {
//    val (logCoordOps, clusterSize, entries: Observable[AppendEntries[A]]) = stateRef.transformAndExtract { state =>
//      state.asLeader match {
//        case None =>
//          val pear = (Option.empty[LogCoords], state.node.clusterSize, Observable.raiseError(state.node.leaderOpinion.asError))
//          pear -> state
//        case Some(leader) =>
////          val commitLogState = commitLog.append(data)
////
////          val entries: Seq[AppendEntries[A]] = leader.makeAppend(data, commitLogState)
////          val pear = (Option(logCoords), leader.clusterSize, Observable.fromIterable(entries))
////          pear -> state
//          ???
//      }
//    }
//
//    val replies = entries.concatMapDelayErrors { entry =>
//      send(entry).collect {
//        case r: AppendEntriesReply => r
//      }
//    }
//
//    logCoordOps match {
//      case None =>
//        entries
//          .map { r =>
//            val res: AppendResult = ???
//            res
//          }
//          .toReactivePublisher[AppendResult]
//      case Some(logCoords) =>
//        val appendResults: Observable[AppendResult] = replies.foldLeftF(AppendResult(logCoords, Map.empty, clusterSize)) {
//          case (aer, reply) =>
//            onMessage(reply)
//            aer.copy(appended = aer.appended.updated(reply.from, reply.success))
//        }
//        appendResults.toReactivePublisher[AppendResult]
//    }
//
//  }
//
//  private def triggerInLock(action: RaftState.ActionResult) = {
//    logger.debug(s"trigger($action)")
//    action match {
//      case LogMessage(explanation, true)   => logger.warn(explanation)
//      case LogMessage(explanation, false)  => logger.info(explanation)
//      case SendMessage(msg)                => send(msg).foreach(onMessage)
//      case AppendLogEntry(coords, data: A) => commitLog.append(coords, data)
//      case CommitLogEntry(coords)          => commitLog.commit(coords)
//      case ResetSendHeartbeatTimeout =>
//        val c = timer.resetSendHeartbeatTimeout(this, sendHeartbeatTimeoutOpt)
//        sendHeartbeatTimeoutOpt = Option(c)
//      case ResetElectionTimeout =>
//        val c = timer.resetReceiveHeartbeatTimeout(this, receivedHeartbeatTimeoutOpt)
//        receivedHeartbeatTimeoutOpt = Option(c)
//    }
//  }
//
//  override def onSendHeartbeatTimeout() = {
//    update(RaftState.OnSendHeartbeatTimeout)
//    ???
//  }
//
//  override def onReceiveHeartbeatTimeout() = ???
//
//  private def onMessage(msg: RaftMessage): Unit = update(RaftState.MessageReceived(msg))
//
//  private def update(msg: RaftState.Action[_]) = {
//    logger.debug(s"update($msg)")
//    stateRef.transform { state =>
//      val (newState, action) = state.update(msg, sched.clockRealTime(TimeUnit.MILLISECONDS))
//      action.foreach(triggerInLock)
//      newState
//    }
//  }
//
//  override def addNode(name: String) = {
//    stateRef.transformAndExtract[Boolean] { state =>
//      state.addNode(Peer(name)).fold(false -> state) { newState =>
//        membershipObserver.onNext(ClusterEvent.nodeAdded(name))
//        true -> newState
//      }
//    }
//  }
//
//  override def removeNode(name: String) = {
//    stateRef.transformAndExtract[Boolean] { state =>
//      state.removeNode(name).fold(false -> state) { newState =>
//        membershipObserver.onNext(ClusterEvent.nodeRemoved(name))
//        true -> newState
//      }
//    }
//  }
//}
