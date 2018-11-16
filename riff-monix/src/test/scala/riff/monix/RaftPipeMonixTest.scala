package riff.monix

import monix.execution.Cancelable
import monix.reactive.{Observable, Observer}
import riff.RaftPipe
import riff.raft.AppendStatus
import riff.raft.log.LogCoords
import riff.raft.messages.{AddressedMessage, RaftMessage, ReceiveHeartbeatTimeout, RequestVote}
import riff.raft.node.RoleCallback.NewLeaderEvent
import riff.raft.node._
import riff.raft.timer.{RaftClock, RandomTimer}
import riff.reactive.{ReactivePipe, TestListener}

import scala.concurrent.duration._

class RaftPipeMonixTest extends RiffMonixSpec {

  "RaftPipe.client" should {
    "elect a leader in a five node cluster" in {
      implicit val clock = newClock

      val fiveNodeCluster = RaftPipeMonix.inMemoryClusterOf[String](5)

      val oneNode = fiveNodeCluster.head._2.handler

      // also observe cluster events
      var leaderEventOpt: Option[NewLeaderEvent] = None
      oneNode.roleCallback.asInstanceOf[ObservableState].asObservable.foreach {
        case newLeader: NewLeaderEvent => leaderEventOpt = Option(newLeader)
        case _ =>
      }

      try {
        fiveNodeCluster.values.foreach(_.resetReceiveHeartbeat())

        val leader = eventually {
          fiveNodeCluster.values.find(_.handler.state().isLeader).get
        }

        val leaderEvent = eventually {
          leaderEventOpt.get
        }

        val leaderTerm = leader.handler.currentTerm()
        if (leaderTerm == leaderEvent.term) {
          leaderEvent.leaderId shouldBe leader.nodeId
        }

      } finally {
        fiveNodeCluster.values.foreach(_.close())
      }
    }
    "send notifications when the append responses are received" in {
//      implicit val clock = newClock
      import concurrent.duration._
      implicit val clock = MonixClock(1.second, RandomTimer(2.seconds, 4.seconds))

      val fiveNodeCluster = RaftPipeMonix.inMemoryClusterOf[String](5)

      try {
        fiveNodeCluster.values.foreach(_.resetReceiveHeartbeat())

        val leader = eventually {
          fiveNodeCluster.values.find(_.handler.state().isLeader).get
        }

        var done = false
        val results: Observable[AppendStatus] = leader.client.append("input").doOnComplete { () => done = true
        }

        eventually {
          done shouldBe true
        }
      } finally {
        fiveNodeCluster.values.foreach(_.close())
      }
    }
  }

  "RaftPipeMonix.publisherFor" should {
    "publisher events destined for a particular node" in {
      implicit val clock = newClock
      val node: RaftNode[String] = RaftNode.inMemory[String]("test").withCluster(RaftCluster("first", "second"))
      val pipeUnderTest: RaftPipe[String, Observer, Observable, Observable, RaftNode[String]] = RaftPipeMonix.raftPipeForNode(node)

      try {

        import riff.reactive.AsPublisher.syntax._
        val forFirst = pipeUnderTest.publisherFor("first")
        val firstInputs = forFirst.subscribeWith(new TestListener[RaftMessage[String]](10, 100))

        val forSecond = pipeUnderTest.publisherFor("second")
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
  "RaftPipeMonix.pipeForNode" should {

    "publish the results of the inputs" in {
      implicit val clock = newClock
      val node = RaftNode.inMemory[String]("test").withCluster(RaftCluster("peer node A", "pier node B"))
      val pipeUnderTest: ReactivePipe[RaftMessage[String], RaftNodeResult[String], Observer, Observable] = RaftPipeMonix.pipeForNode(node)

      try {

        val listener = pipeUnderTest.subscribeWith(new TestListener[RaftNodeResult[String]](10, 100))
        val pi = pipeUnderTest.input

        pi.onNext(ReceiveHeartbeatTimeout)

        eventually {
          listener.received.size shouldBe 1
        }

        val requestVote = RequestVote(1, LogCoords.Empty)
        val List(AddressedRequest(requests)) = listener.received.toList

        requests should contain only (("pier node B" -> requestVote), ("peer node A" -> requestVote))
      } finally {
        pipeUnderTest.close()
      }
    }
  }
  "RaftPipeMonix" should {

    "construct an endpoint which we can used to communicate w/ other endpoints" ignore {

      Given("Two raft nodes linked together")

      implicit val everyonesClock = RaftClock.Default
      val cluster = RaftPipeMonix.asCluster[String](RaftNode.inMemory("a"), RaftNode.inMemory("b"))

      try {
        When("We first reset their clocks")
        cluster.values.foreach(_.resetReceiveHeartbeat())

        Then("Eventually one will become leader")
        val leader = eventually {
          val found = cluster.values.find(_.handler.state().isLeader)
          found.nonEmpty shouldBe true
          found.get
        }

        val followers = cluster.values.filter(_ != leader)

        val appendResultPublisher = leader.client.append("Hello", "World")
        val appendResults: TestListener[AppendStatus] = new TestListener[AppendStatus](10, 100)
        appendResultPublisher.subscribe(Observer.fromReactiveSubscriber(appendResults, new Cancelable { override def cancel(): Unit = appendResults.cancel() }))

        eventually {
          appendResults.completed shouldBe true
        }

        appendResults.received.size shouldBe cluster.size

        eventually {
          followers.foreach { follower =>
            val secondaryValues: Array[String] = follower.handler.log.entriesFrom(1).map(_.data)
            secondaryValues should contain inOrderOnly ("Hello", "World")
          }
        }

        eventually {
          leader.handler.log.latestCommit() shouldBe 2
        }

      } finally {
        cluster.values.foreach(_.close())
        everyonesClock.close()
      }
    }
  }
}
