package riff.monix
import monix.execution.Ack
import monix.reactive.{Observable, Observer}
import riff.raft.log.LogCoords
import riff.raft.messages.{RaftMessage, ReceiveHeartbeatTimeout, RequestVote, TimerMessage}
import riff.raft.node.{AddressedRequest, RaftCluster, RaftNode, RaftNodeResult}
import riff.reactive.{ReactivePipe, TestListener}

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

class ObservableTimerCallbackTest extends RiffMonixSpec {

  "ObservableTimerCallback.onReceiveHeartbeatTimeout" should {
    "trigger an event to observers" in {
      val timer = new ObservableTimerCallback
      val received = ListBuffer[TimerMessage]()

      timer.subscribe(new Observer[TimerMessage] {
        override def onNext(elem: TimerMessage): Future[Ack] = {
          received += elem
          Ack.Continue
        }
        override def onError(ex: Throwable): Unit = ???
        override def onComplete(): Unit = ???
      })

      // call the method under test
      timer.onReceiveHeartbeatTimeout()

      eventually {
        received should contain only (ReceiveHeartbeatTimeout)
      }
    }

  }
  "ObservableTimerCallback" should {

    "publish vote requests to all subscribed when using an ObservableTimerCallback" in {
      implicit val clock = newClock

      val timer = new ObservableTimerCallback
      val node = RaftNode.inMemory[String]("test").withCluster(RaftCluster("a", "b")).withTimerCallback(timer)

      val pipeUnderTest: ReactivePipe[RaftMessage[String], RaftNodeResult[String], Observer, Observable] = RaftPipeMonix.pipeForNode(node)

      try {

        val listener = pipeUnderTest.subscribeWith(new TestListener[RaftNodeResult[String]](10, 100))

        timer.subscribe(pipeUnderTest.input)
        timer.onReceiveHeartbeatTimeout()

        eventually {
          listener.received.size should be > 0
        }

        val List(AddressedRequest(requests)) = listener.received.toList
        requests.size shouldBe 2
        val terms = requests.map {
          case (_, RequestVote(term, LogCoords.Empty)) => term
        }
        withClue(terms.toString) {
          terms.toSet.size shouldBe 1
        }

      } finally {
        pipeUnderTest.close()
      }
    }
  }
}
