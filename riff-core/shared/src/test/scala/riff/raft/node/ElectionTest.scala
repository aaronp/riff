package riff.raft.node
import riff.RiffSpec
import riff.raft.log.LogCoords
import riff.raft.messages._

class ElectionTest extends RiffSpec {

  "RaftNode election" should {
    "transition nodes from followers to candidates and leaders from an initial state" in {
      Given("A cluster of three nodes")
      val cluster       = TestCluster(3)
      val List(a, b, c) = cluster.clusterNodes.ensuring(_.forall(_.persistentState.currentTerm == 0))

      When(s"${a.nodeKey} receives a heartbeat timeout")
      val AddressedRequest(requestVotes) = a.onTimerMessage(ReceiveHeartbeatTimeout)
      requestVotes.size shouldBe 2

      Then(s"${a.nodeKey} should be a candidate")
      a.state().role shouldBe Candidate
      b.state().role shouldBe Follower
      c.state().role shouldBe Follower

      And(s"${a.nodeKey} should send vote requests to ${b.nodeKey} and ${c.nodeKey}")
      val replies: Map[String, RaftNode[Int]#Result] = cluster.sendMessages(a.nodeKey, requestVotes)
      replies.keySet should contain only (b.nodeKey, c.nodeKey)

      When(s"${a.nodeKey} gets its replies from ${b.nodeKey} and ${c.nodeKey}")
      val leaderResultsAfterHavingAppliedTheResponses = replies.map {
        case (from, AddressedResponse(toLeader, voteResponse)) =>
          toLeader shouldBe a.nodeKey
          a.onMessage(from, voteResponse)
        case other => fail(s"Expected an AddressedResponse but got $other")
      }

      leaderResultsAfterHavingAppliedTheResponses should not be (empty)

      Then(s"It should send a first heartbeat message to ${b.nodeKey} and ${c.nodeKey} w/ the new term")
      val firstHeartbeatRequests = cluster.asHeartbeats(leaderResultsAfterHavingAppliedTheResponses)
      firstHeartbeatRequests.keySet should contain only (b.nodeKey, c.nodeKey)

      Then(s"${a.nodeKey} should be the leader")
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
        n.nodeKey -> result.toMap
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
          When(s"${x.nodeKey} votes for ${y.nodeKey}")
          val msgsFromX: Map[String, RaftRequest[Int]]              = voteMsgsByKey(x.nodeKey)
          val xMsgsToY: RaftRequest[Int]                            = msgsFromX(y.nodeKey)
          val AddressedResponse(replyTo, resp: RequestVoteResponse) = y.onMessage(x.nodeKey, xMsgsToY)
          replyTo shouldBe x.nodeKey
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
      val heartbeatMsgsFromA = cluster.electLeader(a.nodeKey)
      cluster.sendMessages(a.nodeKey, heartbeatMsgsFromA.toList)

      And("an entry is appended to the leader and replicated to one other node")
      val AddressedRequest(appendRequests) = a.createAppend(1000)
      val appendRequestsByNode             = appendRequests.toMap
      appendRequestsByNode.keySet should contain only (b.nodeKey, c.nodeKey)
      val AddressedResponse(backToA, appendResp: AppendEntriesResponse) = b.onMessage(a.nodeKey, appendRequestsByNode(b.nodeKey))
      backToA shouldBe a.nodeKey
      appendResp.success shouldBe true
      a.log.latestAppended() shouldBe LogCoords(1, 1)
      b.log.latestAppended() shouldBe LogCoords(1, 1)
      c.log.latestAppended() shouldBe LogCoords.Empty

      When("the node which hasn't yet received the append then times out")
      val heartbeatMsgsFromC = cluster.electLeader(c.nodeKey)

      Then("the remaining two nodes should NOT elect it as a leader, as its log isn't as complete")
      heartbeatMsgsFromC shouldBe empty

      And("the other nodes should increment their terms and become followers, but NOT vote for the candidate")
      a.state().role shouldBe Follower
      b.state().role shouldBe Follower
      c.state().role shouldBe Candidate
      a.persistentState.currentTerm shouldBe 2
      b.persistentState.currentTerm shouldBe 2
      c.persistentState.currentTerm shouldBe 2

      val leaderResetHBCalls = cluster.testTimerFor(a.nodeKey).resetSendHeartbeatCalls()
      leaderResetHBCalls shouldBe 0

      And("The leader should've cancelled its sending heartbeat calls and reset the receiving hb calls")
      cluster.testTimerFor(a.nodeKey).cancelHeartbeatCall() shouldBe 1
    }

  }

}
