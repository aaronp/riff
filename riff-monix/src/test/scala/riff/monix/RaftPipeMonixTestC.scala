package riff.monix

import monix.execution.Ack
import monix.reactive.subjects.Var
import monix.reactive.{Observable, Observer}
import riff.raft.log.LogCoords
import riff.raft.messages._
import riff.raft.node._
import riff.raft.timer.LoggedInvocationClock
import riff.reactive.{ReactivePipe, TestListener}

import scala.collection.mutable.ListBuffer

class RaftPipeMonixTestC extends RiffMonixSpec {

  "RaftPipeMonix.pipeForNode" should {

    "continue to work when one of the input feeds fails" in {

      withScheduler { implicit scheduler =>
        Given("A ReactivePipe whose input subscribes to two feeds")
        implicit val clock = new LoggedInvocationClock // don't actually trigger any heartbeat timeouts - we'll do that manually
        val node           = RaftNode.inMemory[String]("test").withCluster(RaftCluster("peer node A", "pier node B"))
        val pipeUnderTest  = RaftPipeMonix.pipeForNode(node)
        val received       = ListBuffer[RaftNodeResult[String]]()
        pipeUnderTest.output.subscribe { msg =>
          received += msg
          Ack.Continue
        }

        When("One of those feeds completes with an error")
        try {

          val erroredInput: Observable[RaftMessage[String]] = Observable(ReceiveHeartbeatTimeout).endWithError(new Exception("some error"))

          val okInput = Var[RaftMessage[String]](null)
          okInput.filter(_ != null).dump("OK").subscribe(pipeUnderTest.input)
          erroredInput.subscribe(pipeUnderTest.input)

          And("The remaining input sends some messages")
          eventually {
            received.size should be > 0
          }

          val beforeTerm = node.persistentState.currentTerm
          Then("they should still be processed")
          okInput := ReceiveHeartbeatTimeout

          eventually {
            node.persistentState.currentTerm should be > beforeTerm
          }
        } finally {
          pipeUnderTest.close()
        }
      }
    }
    "publish the results of the inputs" in {
      withScheduler { implicit scheduler =>
        implicit val clock                                                                                 = newClock
        val node                                                                                           = RaftNode.inMemory[String]("test").withCluster(RaftCluster("peer node A", "pier node B"))
        val pipeUnderTest: ReactivePipe[RaftMessage[String], RaftNodeResult[String], Observer, Observable] = RaftPipeMonix.pipeForNode(node)

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
}
