package riff.monix
import monix.execution.Scheduler
import riff.monix.log.ObservableLog
import riff.raft.log.HasNodes
import riff.raft.messages.{AddressedMessage, RaftMessage}
import riff.raft.node.{RaftCluster, RaftNode}

/**
  * This is more of a toy/test, as it sets up all the cluster members in the same JVM.
  *
  * Technically it should probably be a test resource, but I've left it here to serve as an example for
  * using a [[MonixNode]].
  *
  * There may be a use-case for doing this as well, where a "local" raft cluster is set up, and by subscribing to the outputs of
  * each local node real requests could be sent to remote ones.
  *
  * @param nodes
  * @param s
  * @tparam A
  */
class MonixCluster[A] private[monix] (override val nodes: Set[MonixNode[A]]) extends HasNodes[A]

object MonixCluster {

  def forNames[A](first: String, theRest: String*)(implicit sched: Scheduler): Unstarted[A] = {
    implicit val clock = MonixClock()

    MonixCluster[A](theRest.toSet + first) { name => //
      val original = RaftNode.inMemory[A](name)
      val log: ObservableLog[A] = new ObservableLog[A](original.log)
      original.withLog(log.cached())
    }
  }

  def apply[A](nodeNames: Set[String])(newNode: String => RaftNode[A]): Unstarted[A] = {

    val router: Router[RaftMessage[A], RaftMessage[A]] = Router[RaftMessage[A]](nodeNames)

    val all = nodeNames.map { name =>
      val peers = nodeNames - name
      val cluster = RaftCluster(peers)
      val pipe: NamedPipe[RaftMessage[A], RaftMessage[A]] = router.pipes(name)
      import pipe.bufferedSubscriber.scheduler

      val monixNode = MonixNode(newNode(name).withCluster(cluster), pipe)

      // subscribe each peer to the addressed output
      peers.foreach { peer =>
        // the output is (recipient / msg). Change that to be (sender / msg)
        val toThisPeer = monixNode.out.collect {
          case AddressedMessage(`peer`, msg) => AddressedMessage(name, msg)
        }

        //.dump(s"from ${name} to $peer")
        toThisPeer.subscribe(router.pipes(peer).bufferedSubscriber)
      }

      monixNode
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
  class Unstarted[A] private[monix] (override val nodes: Set[MonixNode[A]]) extends HasNodes[A] {

    def start(): MonixCluster[A] = {
      nodes.foreach { node =>
        // start the timeout
        node.resetReceiveHeartbeat()
      }
      new MonixCluster(nodes)
    }
  }
}
