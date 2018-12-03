package riff.raft
import org.reactivestreams.Publisher
import riff.RiffThreadedSpec
import riff.raft.log.{LogAppendSuccess, LogCoords, LogEntry}
import riff.raft.messages.{AppendEntriesResponse, _}
import riff.raft.node._
import riff.raft.timer.LoggedInvocationClock
import riff.reactive.AsPublisher.syntax._
import riff.reactive.{FixedPublisher, TestListener}

class AppendStatusTest extends RiffThreadedSpec {

  "AppendStatus.asStatusPublisher" should {

    "complete in a single-node cluster" in {

      Given("An append request and its four peer results from a cluster of 5 nodes")
      val appendResult = LogAppendSuccess(LogCoords(1, 1), LogCoords(1, 1))

      val testValues: List[(RaftMessage[String], RaftNodeResult[String])] = {
        val appendResult = NodeAppendResult(LogAppendSuccess(LogCoords(1, 1), LogCoords(1, 1)), AddressedRequest[String]())
        List(AppendData("first") -> appendResult)
      }

      When("The 'asStatusPublisher' publisher is fed the input/result messages for our node under test")
      val inputs: Publisher[(RaftMessage[String], RaftNodeResult[String])] = FixedPublisher(testValues, allowOnComplete = false)
      val publisherUnderTest: Publisher[AppendStatus]                      = AppendStatus.asStatusPublisher("node under test", 1, appendResult, inputs)

      Then("It should produce a series of append status responses and call onComplete")
      val listener = publisherUnderTest.subscribeWith(new TestListener[AppendStatus]())
      listener.request(100)

      eventually {
        listener.received.size shouldBe 1
      }

      listener.received.head shouldBe AppendStatus(LogAppendSuccess(LogCoords(1, 1), LogCoords(1, 1)),
                                                   Map("node under test" -> AppendEntriesResponse(1, true, 1)),
                                                   Set(LogCoords(1, 1)),
                                                   1,
        false)
      eventually {
        listener.completed shouldBe true
      }
    }

    "complete once all nodes have matched a single append" in {

      Given("An append request and its four peer results from a cluster of 5 nodes")
      val peerNodes = (1 to 4).map(i => s"node $i")

      val appendResult = LogAppendSuccess(LogCoords(1, 1), LogCoords(1, 1))

      // here we fake up some test inputs, but we use a real RaftNode in another test case to provide/demonstrate/test
      // an alternative means to producing input/outputs
      val testValues: List[(RaftMessage[String], RaftNodeResult[String])] = {
        val clusterMessages = peerNodes.map { peer => //
          (peer, AppendEntries(previous = LogCoords.Empty, term = 1, commitIndex = 0, entries = Array(LogEntry(1, "first"))))
        }

        val appendResponses = peerNodes.map { peer => //
          (peer, AppendEntries(previous = LogCoords.Empty, term = 1, commitIndex = 0, entries = Array(LogEntry(1, "first"))))

          // in setting up our test data, we know the 2nd node creates 3/5, and so use committed value test data
          val commitResponse = peer match {
            case "node 2" =>
              LeaderCommittedResult[String](List(LogCoords(1, 1)), NoOpResult("ok"))
            case _ =>
              LeaderCommittedResult[String](List(), NoOpResult("ok"))
          }
          AddressedMessage(peer, AppendEntriesResponse(1, true, 1)) -> commitResponse
        }

        val appendResult = NodeAppendResult(LogAppendSuccess(LogCoords(1, 1), LogCoords(1, 1)), AddressedRequest(clusterMessages))
        (AppendData("first"), appendResult) +: appendResponses.toList
      }

      When("The 'asStatusPublisher' publisher is fed the input/result messages for our node under test")

      // we want to test that 'onComplete' works once we get all our AppendEntryResponse values.
      // to prove that our code is working correctly as opposed to getting an 'onComplete' from our
      // test publisher we don't let the underlying FixedPublisher to signal it's complete
      //
      // As an aside, I wouldn't have noticed this had I not have written the test here first and saw that it passed
      // before implementing the 'onComplete' logic for AppendStatus
      val inputs: Publisher[(RaftMessage[String], RaftNodeResult[String])] = FixedPublisher(testValues, allowOnComplete = false)
      val publisherUnderTest: Publisher[AppendStatus]                      = AppendStatus.asStatusPublisher("node under test", peerNodes.size + 1, appendResult, inputs)

      Then("It should produce a series of append status responses and call onComplete")
      val listener = publisherUnderTest.subscribeWith(new TestListener[AppendStatus]())
      listener.request(100)

      eventually {
        listener.received.size shouldBe 5
      }

      listener.received.head shouldBe AppendStatus(LogAppendSuccess(LogCoords(1, 1), LogCoords(1, 1)), Map("node under test" -> AppendEntriesResponse(1, true, 1)), Set.empty, 5, true)

      val expectedResponseKeysAndCommitted = listener.received.toList.map { r =>
        r.leaderAppendResult shouldBe appendResult
        r.clusterSize shouldBe 5
        r.appended.values.foreach(_ shouldBe AppendEntriesResponse.ok(1, 1))
        (r.appended.keySet, r.committed, r.numberOfPeersContainingCommittedIndex)
      }

      expectedResponseKeysAndCommitted shouldBe List(
        (Set("node under test"), false, 0),
        (Set("node under test", "node 1"), false, 0),
        (Set("node under test", "node 1", "node 2"), true, 3),
        (Set("node under test", "node 1", "node 2", "node 3"), true, 4),
        (Set("node under test", "node 1", "node 2", "node 3", "node 4"), true, 5)
      )
      eventually {
        listener.completed shouldBe true
      }
    }

    "published acknowledged responses from a single append" in {

      Given("An append request and its four peer results from a cluster of 5 nodes")
      val peerNodes = (1 to 4).map(i => s"node $i")

      val nodeUsedToGetTestData = {
        implicit val clock = new LoggedInvocationClock
        val node           = RaftNode.inMemory[String]("test").withCluster(RaftCluster(peerNodes))

        // make the node a leader. Again, we could do this by setting the node status directly, but going this
        // way means that we use the 'real' methods themselves to get a node into that state. Of course that does
        // mean that, if that breaks or is incorrect, then this seemingly unrelated test will fail. Trade-offs, eh?
        node.onReceiveHeartbeatTimeout()
        peerNodes.foreach { peer => //
          node.onRequestVoteResponse(peer, RequestVoteResponse(node.currentTerm(), true))
        }
        node.state().isLeader shouldBe true
        node
      }

      val input                              = AppendData("first")
      val response: NodeAppendResult[String] = nodeUsedToGetTestData.onAppendData(input)
      response.request.size shouldBe 4
      val appendResult = response.appendResult.asInstanceOf[LogAppendSuccess]

      // we can 'fake' up some data, but it's more realistic to use an existing, real node to create it
      val testValues: List[(RaftMessage[String], RaftNodeResult[String])] = {
        val fakeResponses: List[(AddressedMessage[String], RaftNodeResult[String])] = peerNodes.toList.map { node => //

          // let one of the nodes return 'false', in the case for instance where it's just been started
          val peerNodeResponse = node match {
            case "node 1" => AppendEntriesResponse.fail(nodeUsedToGetTestData.currentTerm())
            case _        => AppendEntriesResponse.ok(nodeUsedToGetTestData.currentTerm(), 1)
          }
          val resp                           = AddressedMessage[String](node, peerNodeResponse)
          val result: RaftNodeResult[String] = nodeUsedToGetTestData.onMessage(resp)
          resp -> result
        }

        (input -> response) +: fakeResponses :+ (ReceiveHeartbeatTimeout, NoOpResult("no op"))
      }

      When("The 'asStatusPublisher' publisher is fed the input/result messages for our node under test")
      val inputs: Publisher[(RaftMessage[String], RaftNodeResult[String])] = FixedPublisher(testValues, allowOnComplete = false)
      val pub: Publisher[AppendStatus]                                     = AppendStatus.asStatusPublisher("node under test", 5, appendResult, inputs)

      Then("It should produce a series of append status responses and call onComplete")
      val listener = pub.subscribeWith(new TestListener[AppendStatus]())
      listener.request(10)

      eventually {
        withClue("failed ApppendEntryResponse values have match index 0, and so we shouldn't receive a failed node 1 response, thus 4 out of 5 append values") {

          listener.received.size shouldBe 4
        }
      }

      listener.received.head shouldBe AppendStatus(LogAppendSuccess(LogCoords(1, 1), LogCoords(1, 1)), Map("node under test" -> AppendEntriesResponse(1, true, 1)), Set.empty, 5, true)

      val expectedResponseKeysAndCommitted = listener.received.toList.map { r =>
        r.leaderAppendResult shouldBe appendResult
        r.clusterSize shouldBe 5
        r.appended.foreach {
          case ("node 1", resp) =>
            resp shouldBe AppendEntriesResponse.fail(nodeUsedToGetTestData.currentTerm())
          case (_, resp) =>
            resp shouldBe AppendEntriesResponse.ok(nodeUsedToGetTestData.currentTerm(), 1)
        }
        (r.appended.keySet, r.committed, r.numberOfPeersContainingCommittedIndex)
      }

      expectedResponseKeysAndCommitted shouldBe List(
        (Set("node under test"), false, 0),
        (Set("node under test", "node 2"), false, 0),
        (Set("node under test", "node 2", "node 3"), true, 3), // quorum! all replies thereafter are still committed
        (Set("node under test", "node 2", "node 3", "node 4"), true, 4)
      )

      withClue(
        "this test sends a failed response from node 1, " +
          "so in this instance we're still waiting for a successful ack to match (which we don't send in the test), " +
          "and so the listener should never complete") {
        listener.completed shouldBe false
      }
    }
  }
}
