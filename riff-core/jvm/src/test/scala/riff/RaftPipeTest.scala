package riff
import org.reactivestreams.{Publisher, Subscriber}
import riff.raft.log.LogCoords
import riff.raft.messages.{AddressedMessage, RaftMessage, ReceiveHeartbeatTimeout, RequestVote}
import riff.raft.node.{AddressedRequest, RaftCluster, RaftNode, RaftNodeResult}
import riff.reactive.{ReactivePipe, ReactiveTimerCallback, TestListener}

import scala.concurrent.duration._

class RaftPipeTest extends RiffThreadedSpec {

  "RaftPipe.publisherFor" should {
    "publisher events destined for a particular node" in {
      withExecCtxt { implicit execCtxt =>
        withClock(testTimeout) { implicit clock =>
          val node = RaftNode.inMemory[String]("test").withCluster(RaftCluster("first", "second"))
          val pipeUnderTest: RaftPipe[String, Subscriber, Publisher, Publisher, RaftNode[String]] =
            RaftPipe.raftPipeForNode(node, 1000)

          try {

            import riff.reactive.AsPublisher.syntax._
            val forFirst    = pipeUnderTest.publisherFor("first")
            val firstInputs = forFirst.subscribeWith(new TestListener[RaftMessage[String]](10, 100))

            val forSecond    = pipeUnderTest.publisherFor("second")
            val secondInputs = forSecond.subscribeWith(new TestListener[RaftMessage[String]](10, 100))

            // poke the node under test, forcing it to send messages to the cluster (e.g. first and second nodes)
            val in = pipeUnderTest.input
            in.onNext(ReceiveHeartbeatTimeout)

            eventually {
              val List(AddressedMessage("test", RequestVote(term, LogCoords.Empty))) = firstInputs.received.toList
              term should be > 0
            }
            eventually {
              val List(AddressedMessage("test", RequestVote(term, LogCoords.Empty))) = secondInputs.received.toList
              term should be > 0
            }
          } finally {
            pipeUnderTest.close()
          }
        }
      }
    }
  }
  "RaftPipe.pipeForNode" should {

    "publish the results of the inputs" in {
      withExecCtxt { implicit ctxt =>
        withClock(testTimeout) { implicit clock =>
          val node = RaftNode.inMemory[String]("test").withCluster(RaftCluster("peer node A", "pier node B"))
          val pipeUnderTest: ReactivePipe[RaftMessage[String], RaftNodeResult[String], Subscriber, Publisher] =
            RaftPipe.pipeForNode(node, 1000)

          try {

            val listener = pipeUnderTest.subscribeWith(new TestListener[RaftNodeResult[String]](10, 100))
            val pi       = pipeUnderTest.input

            pi.onNext(ReceiveHeartbeatTimeout)

            eventually {
              listener.received.size shouldBe 1
            }

            val requestVote                      = RequestVote(1, LogCoords.Empty)
            val List(AddressedRequest(requests)) = listener.received.toList

            requests should contain only (("pier node B" -> requestVote), ("peer node A" -> requestVote))
          } finally {
            pipeUnderTest.close()
          }
        }
      }
    }

    "publish vote requests to all subscribed when using a ReactiveTimerCallback" in {

      withExecCtxt { implicit ctxt =>
        withClock(10.millis) { implicit clock =>
          val timer = ReactiveTimerCallback()
          val node  = RaftNode.inMemory[String]("test").withCluster(RaftCluster("a", "b")).withTimerCallback(timer)

          val pipeUnderTest: ReactivePipe[RaftMessage[String], RaftNodeResult[String], Subscriber, Publisher] =
            RaftPipe.pipeForNode(node, 1000)

          try {

            val listeners = (0 to 5).map { _ =>
              val listener = pipeUnderTest.subscribeWith(new TestListener[RaftNodeResult[String]])
              listener.request(10)
              listener
            }

            timer.subscribe(pipeUnderTest.input)
            timer.onReceiveHeartbeatTimeout()

            eventually {
              listeners.foreach(_.received.size should be > 0)
            }

            listeners.foreach { listener =>
              val List(AddressedRequest(requests)) = listener.received.toList
              requests.size shouldBe 2
              val terms = requests.map {
                case (_, RequestVote(term, LogCoords.Empty)) => term
                case other                                   => fail(s"should all be Request Vote requests: $other")
              }
              withClue(terms.toString) {
                terms.toSet.size shouldBe 1
              }
            }
          } finally {
            timer.close()
            pipeUnderTest.close()
          }
        }
      }
    }
  }
}
