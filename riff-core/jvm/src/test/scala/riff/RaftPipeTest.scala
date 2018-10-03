package riff
import org.reactivestreams.{Publisher, Subscriber}
import riff.raft.log.LogCoords
import riff.raft.messages.{AddressedMessage, RaftMessage, ReceiveHeartbeatTimeout, RequestVote}
import riff.raft.node._
import riff.raft.timer.{RaftClock, RandomTimer}
import riff.reactive.{ReactivePipe, ReactiveTimerCallback, TestListener}

import scala.concurrent.duration._

class RaftPipeTest extends RiffThreadedSpec {

  override def testTimeout = 5.seconds

  "RaftPipe.publisherFor" should {
    "publisher events destined for a particular node" in {
      withClock(testTimeout) { implicit clock =>
        val node = RaftNode.inMemory[String]("test").withCluster(RaftCluster("first", "second"))
        val pipeUnderTest = RaftPipe.raftPipeForNode(node)

        import riff.reactive.AsPublisher.syntax._
        val forFirst = pipeUnderTest.publisherFor("first")
        val firstInputs = forFirst.subscribeWith(new TestListener[RaftMessage[String]](10, 100))

        val forSecond = pipeUnderTest.publisherFor("second")
        val secondInputs = forSecond.subscribeWith(new TestListener[RaftMessage[String]](10, 100))

        // poke the node under test, forcing it to send messages to the cluster (e.g. first and second nodes)
        pipeUnderTest.input.onNext(ReceiveHeartbeatTimeout)

        eventually {
          val List(AddressedMessage("test", RequestVote(term, LogCoords.Empty))) = firstInputs.received.toList
          term should be > 0
        }
        eventually {
          val List(AddressedMessage("test", RequestVote(term, LogCoords.Empty))) = secondInputs.received.toList
          term should be > 0
        }
      }
    }
  }
  "RaftPipe.pipeForNode" should {
    "publish the results of the inputs" in {
      withClock(testTimeout) { implicit clock =>
        val node = RaftNode.inMemory[String]("test").withCluster(RaftCluster("peer node A", "pier node B"))
        val pipeUnderTest = RaftPipe.pipeForNode(node)

        val listener = pipeUnderTest.subscribeWith(new TestListener[RaftNodeResult[String]](10, 100))
        val pi = pipeUnderTest.input

        pi.onNext(ReceiveHeartbeatTimeout)

        eventually {
          listener.received.size shouldBe 1
        }

        val requestVote = RequestVote(1, LogCoords.Empty)
        val List(AddressedRequest(requests)) = listener.received.toList

        requests should contain only (("pier node B" -> requestVote), ("peer node A" -> requestVote))
      }
    }

    "publish vote requests to all subscribed when using a ReactiveTimerCallback" in {
      withClock(10.millis) { implicit clock =>
        val timer = ReactiveTimerCallback()
        val node = RaftNode.inMemory[String]("test").withCluster(RaftCluster("a", "b")).withTimerCallback(timer)

        val pipeUnderTest: ReactivePipe[RaftMessage[String], RaftNodeResult[String], Subscriber, Publisher] =
          RaftPipe.pipeForNode(node)

        val listeners = (0 to 5).map { _ =>
          val listener = pipeUnderTest.subscribeWith(new TestListener[RaftNodeResult[String]])
          listener.request(10)
          listener
        }

        timer.subscribe(pipeUnderTest.input)
        timer.onReceiveHeartbeatTimeout()

        eventually {
          listeners.foreach { listener =>
            listener.received.size should be > 0
          }
        }

        listeners.foreach { listener =>
          val List(AddressedRequest(requests)) = listener.received.toList
          requests.size shouldBe 2
          val terms = requests.map {
            case (_, RequestVote(term, LogCoords.Empty)) => term
          }
          withClue(terms.toString) {
            terms.toSet.size shouldBe 1
          }
        }
      }
    }
  }

  "RaftPipe" should {

    "construct an endpoint which we can used to communicate w/ other endpoints" in {

      Given("Two raft nodes linked together")

      implicit val everyonesClock = RaftClock.Default
      val cluster = RaftPipe.asCluster[String](RaftNode.inMemory("a"), RaftNode.inMemory("b"))
      try {

        When("We first reset their clocks")
        cluster.values.foreach(_.resetReceiveHeartbeat())

        Then("Eventually one will become leader")
        val leader: RaftPipe[String, Subscriber, Publisher, Publisher] = eventually {
          val found = cluster.values.find(_.node.state().isLeader)
          found.nonEmpty shouldBe true
          found.get
        }

        val followers: Iterable[RaftPipe[String, Subscriber, Publisher, Publisher]] = cluster.values.filter(_ != leader)

        leader.client.append("Hello", "World")

        eventually {
          followers.foreach { follower =>
            val secondaryValues: Array[String] = follower.node.log.entriesFrom(1).map(_.data)
            secondaryValues should contain inOrderOnly ("Hello", "World")
          }
        }

        eventually {
          leader.node.log.latestCommit() shouldBe 2
        }

      } finally {
        cluster.values.foreach { rn =>
          rn.node.cancelReceiveHeartbeat()
          rn.node.cancelSendHeartbeat()
        }
        everyonesClock.close()
      }
    }
  }

  private def withClock[T](hbTimeout: FiniteDuration)(thunk: RaftClock => T): T = {
    val clock = RaftClock(hbTimeout, RandomTimer(hbTimeout + 200.millis, hbTimeout + 300.millis))
    try {
      thunk(clock)
    } finally {
      clock.close()
    }
  }
}
