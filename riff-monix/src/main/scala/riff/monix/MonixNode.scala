package riff.monix

import monix.execution.Scheduler
import monix.reactive.Observable
import riff.raft.messages.RequestOrResponse
import riff.raft.node._

class MonixNode[A] private (
  val raftNode: RaftNode[A],
  val pipe: NamedPipe[RaftStreamInput, RaftStreamInput],
  val out: Observable[AddressedInput[A]])

object MonixNode {

  def forNames[A](first: String, theRest: String*)(
    implicit s: Scheduler = RiffSchedulers.computation.scheduler): Set[MonixNode[A]] = {
    implicit val clock = MonixClock()

    apply(theRest.toSet + first) { name =>
      RaftNode.inMemory(name)
    }
  }

  def apply[A](nodeNames: Set[String])(newNode: String => RaftNode[A])(implicit s: Scheduler): Set[MonixNode[A]] = {

    val router = Router[RaftStreamInput](nodeNames)

    nodeNames.map { name =>
      val peers = nodeNames - name
      val pipe = router.pipes(name)

      // use a different callback other than the node itself
      val callback = new ObservableCallback
      callback.sendTimeout.subscribe(pipe.bufferedSubscriber)
      callback.receiveTimeouts.subscribe(pipe.bufferedSubscriber)
      val raftNode: RaftNode[A] = newNode(name).withCluster(RaftCluster(peers)).withTimerCallback(callback)
      raftNode.resetReceiveHeartbeat()

      val out: Observable[AddressedInput[A]] = pipe.output.dump(s"$name handler").flatMap {
        case TimerInput(msg) =>
          val result = raftNode.onTimerMessage(msg)
          RaftStreamInput.resultAsObservable(result)
        case AddressedInput(from, msg: RequestOrResponse[A]) =>
          val result = raftNode.onMessage(from, msg)
          RaftStreamInput.resultAsObservable(result)
      }

      // subscribe each peer to the addressed output
      peers.foreach { peer =>
        // the output is (recipient / msg). Change that to be (sender / msg)
        val toThisPeer = out.collect {
          case AddressedInput(`peer`, msg) => AddressedInput(name, msg)
        }

        toThisPeer.subscribe(router.pipes(peer).bufferedSubscriber)
      }

      new MonixNode[A](raftNode, pipe, out)
    }
  }
}
