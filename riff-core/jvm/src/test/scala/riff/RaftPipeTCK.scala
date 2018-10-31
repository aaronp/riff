package riff

import org.reactivestreams.{Publisher, Subscriber}
import riff.raft.AppendStatus
import riff.raft.log.LogCoords
import riff.raft.messages._
import riff.raft.node._
import riff.raft.timer.{RaftClock, RandomTimer}
import riff.reactive.AsPublisher.syntax._
import riff.reactive.{ReactivePipe, ReactiveTimerCallback, TestListener}

import scala.concurrent.duration._

trait RaftPipeTCK extends RiffThreadedSpec {

  "RaftPipe.client" ignore {
    "send notifications when the append responses are received" in {
      withClock(10.millis) { implicit clock =>
        val fiveNodeCluster = RaftPipe.inMemoryClusterOf[String](5)
        try {
          fiveNodeCluster.values.foreach(_.resetReceiveHeartbeat())

          val leader = eventually {
            fiveNodeCluster.values.find(_.handler.state().isLeader).get
          }
          val results = leader.client.append("input")
          val listener = results.subscribeWith(new TestListener[AppendStatus](10, 10))
          eventually {
            listener.completed shouldBe true
          }
//          println(listener.received.size)
//          listener.received.foreach(println)
//          println()
        } finally {
          fiveNodeCluster.values.foreach(_.close())
        }
//        println("checking...")
//        Thread.sleep(1000000)
//        println("done")
      }
    }
  }

  "RaftPipe.publisherFor" should {
    "publisher events destined for a particular node" in {
      withClock(testTimeout) { implicit clock =>
        val node = RaftNode.inMemory[String]("test").withCluster(RaftCluster("first", "second"))
        val pipeUnderTest: RaftPipe[String, Subscriber, Publisher, Publisher, RaftNode[String]] =
          RaftPipe.raftPipeForNode(node, 1000)

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
  }
  "RaftPipe.pipeForNode" should {

    "publish the results of the inputs" in {
      withClock(testTimeout) { implicit clock =>
        val node = RaftNode.inMemory[String]("test").withCluster(RaftCluster("peer node A", "pier node B"))
        val pipeUnderTest: ReactivePipe[RaftMessage[String], RaftNodeResult[String], Subscriber, Publisher] =
          RaftPipe.pipeForNode(node, 1000)

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

    "publish vote requests to all subscribed when using a ReactiveTimerCallback" in {
      withClock(10.millis) { implicit clock =>
        val timer = ReactiveTimerCallback()
        val node = RaftNode.inMemory[String]("test").withCluster(RaftCluster("a", "b")).withTimerCallback(timer)

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
  "RaftPipe" should {

    "construct an endpoint which we can used to communicate w/ other endpoints" ignore {

      Given("Two raft nodes linked together")

      implicit val everyonesClock = RaftClock.Default
      val cluster = RaftPipe.asCluster[String](RaftNode.inMemory("a"), RaftNode.inMemory("b"))

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

        val appendResultPublisher: Publisher[AppendStatus] = leader.client.append("Hello", "World")
        val appendResults: TestListener[AppendStatus] = appendResultPublisher.subscribeWith(new TestListener[AppendStatus](10, 100))

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

  private def withClock[T](hbTimeout: FiniteDuration)(thunk: RaftClock => T): T = {
    val clock = RaftClock(hbTimeout, RandomTimer(hbTimeout + 200.millis, hbTimeout + 300.millis))
    try {
      thunk(clock)
    } finally {
      clock.close()
    }
  }
}
