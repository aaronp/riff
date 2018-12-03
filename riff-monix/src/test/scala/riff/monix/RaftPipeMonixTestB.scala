package riff.monix

import monix.reactive.{Observable, Observer}
import riff.RaftPipe
import riff.monix.log.ObservableLog
import riff.raft.log.LogCoords
import riff.raft.messages._
import riff.raft.node._
import riff.reactive.TestListener

class RaftPipeMonixTestB extends RiffMonixSpec {

  "RaftPipeMonix.publisherFor" should {
    "publisher events destined for a particular node" in {

      withScheduler { implicit scheduler =>
        implicit val clock = newClock
        val node: RaftNode[String] = {
          val n = RaftNode.inMemory[String]("test").withCluster(RaftCluster("first", "second"))
          n.withLog(ObservableLog(n.log))
        }
        val pipeUnderTest: RaftPipe[String, Observer, Observable, Observable, RaftNode[String]] = RaftPipeMonix.raftPipeForHandler(node)

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
