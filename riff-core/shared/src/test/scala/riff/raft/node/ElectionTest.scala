package riff.raft.node
import riff.RiffSpec
import riff.raft.log.{LogAppendSuccess, LogCoords}
import riff.raft.messages._

class ElectionTest extends RiffSpec {

  "RaftNode election" should {
    "transition nodes from followers to candidates and leaders from an initial state" in {
      Given("A cluster of three nodes")
      val cluster       = TestCluster(3)
      val List(a, b, c) = cluster.clusterNodes.ensuring(_.forall(_.persistentState.currentTerm == 0))

      When(s"${a.nodeId} receives a heartbeat timeout")
      val AddressedRequest(requestVotes) = a.onTimerMessage(ReceiveHeartbeatTimeout)
      requestVotes.size shouldBe 2

      Then(s"${a.nodeId} should be a candidate")
      a.state().role shouldBe Candidate
      b.state().role shouldBe Follower
      c.state().role shouldBe Follower

      And(s"${a.nodeId} should send vote requests to ${b.nodeId} and ${c.nodeId}")
      val replies: Map[String, RaftNode[Int]#Result] = cluster.sendMessages(a.nodeId, requestVotes)
      replies.keySet should contain only (b.nodeId, c.nodeId)

      When(s"${a.nodeId} gets its replies from ${b.nodeId} and ${c.nodeId}")
      val leaderResultsAfterHavingAppliedTheResponses = replies.map {
        case (from, AddressedResponse(toLeader, voteResponse)) =>
          toLeader shouldBe a.nodeId
          a.handleMessage(from, voteResponse)
        case other => fail(s"Expected an AddressedResponse but got $other")
      }

      leaderResultsAfterHavingAppliedTheResponses should not be (empty)

      Then(s"It should send a first heartbeat message to ${b.nodeId} and ${c.nodeId} w/ the new term")
      val firstHeartbeatRequests = cluster.asHeartbeats(leaderResultsAfterHavingAppliedTheResponses)
      firstHeartbeatRequests.keySet should contain only (b.nodeId, c.nodeId)

      Then(s"${a.nodeId} should be the leader")
      a.state().role shouldBe Leader
      b.state().role shouldBe Follower
      c.state().role shouldBe Follower
      cluster.clusterNodes.foreach(_.persistentState.currentTerm shouldBe 1)
    }
    "not elect a leader in a split-brain, four node election" in {
      Given("A cluster of four nodes")
      val cluster          = TestCluster(4)
      val List(a, b, c, d) = cluster.clusterNodes.ensuring(_.forall(_.persistentState.currentTerm == 0))

      When("All the nodes timeout w/o a heartbeat message at the same time")
      val voteMsgsByKey: Map[String, Map[String, RaftRequest[Int]]] = cluster.clusterNodes.map { n =>
        val AddressedRequest(result) = n.onTimerMessage(ReceiveHeartbeatTimeout)
        n.nodeId -> result.toMap
      }.toMap

      Then("They should all be candidates")
      cluster.clusterNodes.foreach(_.state().role shouldBe Candidate)

      val votes = List(
        a -> b,
        b -> c,
        c -> d,
        d -> a
      )
      val responses = votes.map {
        case (x, y) =>
          When(s"${x.nodeId} votes for ${y.nodeId}")
          val msgsFromX: Map[String, RaftRequest[Int]]              = voteMsgsByKey(x.nodeId)
          val xMsgsToY: RaftRequest[Int]                            = msgsFromX(y.nodeId)
          val AddressedResponse(replyTo, resp: RequestVoteResponse) = y.handleMessage(x.nodeId, xMsgsToY)
          replyTo shouldBe x.nodeId
          resp
      }

      Then("All responses should be false as they're all voting in the same term as themselves")
      responses.foreach(_.granted shouldBe false)
    }
    "reject nodes with a shorter log from being elected" in {
      Given("A cluster of three nodes")
      val cluster       = TestCluster(3)
      val List(a, b, c) = cluster.clusterNodes.ensuring(_.forall(_.persistentState.currentTerm == 0))

      When("One node becomes the leader")
      val heartbeatMsgsFromA = cluster.electLeader(a.nodeId)
      cluster.sendMessages(a.nodeId, heartbeatMsgsFromA.toList)

      And("an entry is appended to the leader and replicated to one other node")
      val NodeAppendResult(LogAppendSuccess(LogCoords(1, 1),LogCoords(1, 1), Nil), AddressedRequest(appendRequests)) = a.createAppendFor(1000)
      val appendRequestsByNode             = appendRequests.toMap
      appendRequestsByNode.keySet should contain only (b.nodeId, c.nodeId)
      val AddressedResponse(backToA, appendResp: AppendEntriesResponse) = b.handleMessage(a.nodeId, appendRequestsByNode(b.nodeId))
      backToA shouldBe a.nodeId
      appendResp.success shouldBe true
      a.log.latestAppended() shouldBe LogCoords(1, 1)
      b.log.latestAppended() shouldBe LogCoords(1, 1)
      c.log.latestAppended() shouldBe LogCoords.Empty

      When("the node which hasn't yet received the append then times out")
      val heartbeatMsgsFromC = cluster.electLeader(c.nodeId)

      Then("the remaining two nodes should NOT elect it as a leader, as its log isn't as complete")
      heartbeatMsgsFromC shouldBe empty

      And("the other nodes should increment their terms and become followers, but NOT vote for the candidate")
      a.state().role shouldBe Follower
      b.state().role shouldBe Follower
      c.state().role shouldBe Candidate
      a.persistentState.currentTerm shouldBe 2
      b.persistentState.currentTerm shouldBe 2
      c.persistentState.currentTerm shouldBe 2

      val leaderResetHBCalls = cluster.testTimerFor(a.nodeId).resetSendHeartbeatCalls()
      leaderResetHBCalls shouldBe 1

      And("The leader should've cancelled its sending heartbeat calls and reset the receiving hb calls")
      cluster.testTimerFor(a.nodeId).cancelHeartbeatCall() shouldBe 2
    }

  }

}
