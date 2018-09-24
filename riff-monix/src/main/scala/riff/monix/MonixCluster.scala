package riff.monix
import monix.execution.Scheduler
import monix.reactive.Observable
import riff.monix.log.ObservableLog
import riff.raft.log.HasNodes
import riff.raft.messages.{AddressedMessage, RaftMessage}
import riff.raft.node.{RaftCluster, RaftNode}

class MonixCluster[A] private[monix] (override val nodes: Set[MonixNode[A]])(implicit s: Scheduler) extends HasNodes[A]

object MonixCluster {

  def forNames[A](first: String, theRest: String*)(implicit sched: Scheduler): Unstarted[A] = {
    implicit val clock = MonixClock()

    MonixCluster[A](theRest.toSet + first) { name => //
      val original = RaftNode.inMemory[A](name)
      val log: ObservableLog[A] = new ObservableLog[A](original.log)
      original.withLog(log.cached())
    }
  }

  def apply[A](nodeNames: Set[String])(newNode: String => RaftNode[A])(implicit s: Scheduler): Unstarted[A] = {

    val router: Router[RaftMessage[A], RaftMessage[A]] = Router[RaftMessage[A]](nodeNames)

    val all = nodeNames.map { name =>
      val peers = nodeNames - name
      val pipe: NamedPipe[RaftMessage[A], RaftMessage[A]] = router.pipes(name)

      // use a different callback other than the node itself
      val callback = new ObservableCallback
      callback.sendTimeout.subscribe(pipe.bufferedSubscriber)
      callback.receiveTimeouts.subscribe(pipe.bufferedSubscriber)

      // also attach an 'appendIfLeader' input

      val roleObs = NodeRoleObservable()
      val raftNode: ObservableRaftNode[A] = {
        val node = newNode(name) //
          .withCluster(RaftCluster(peers)) //
          .withTimerCallback(callback) //
          .withRoleCallback(roleObs)

        new ObservableRaftNode[A](node, pipe.bufferedSubscriber)
      }

      val out: Observable[AddressedMessage[A]] =
        pipe.output.map(raftNode.onMessage).flatMap(resultAsObservable)

      // subscribe each peer to the addressed output
      peers.foreach { peer =>
        // the output is (recipient / msg). Change that to be (sender / msg)
        val toThisPeer = out.collect {
          case AddressedMessage(`peer`, msg) => AddressedMessage(name, msg)
        }

        toThisPeer.subscribe(router.pipes(peer).bufferedSubscriber)
      }

      new MonixNode[A](raftNode, pipe, out, callback, roleObs)
    }
    new Unstarted[A](all)
  }

  /**
    * An intermediate state, not really intended to be referenced directly.
    *
    * Just helps expose a DSL to explicitly 'start' the cluster (e.g., trigger the timeouts)
    *
    * @param nodes
    * @param s
    * @tparam A
    */
  class Unstarted[A] private[monix] (override val nodes: Set[MonixNode[A]])(implicit s: Scheduler) extends HasNodes[A] {

    def start(): MonixCluster[A] = {
      nodes.foreach { node =>
        // start the timeout
        node.resetReceiveHeartbeat()
      }
      new MonixCluster(nodes)
    }
  }
}
