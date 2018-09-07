package riff.raft.node
import riff.RiffSpec
import riff.raft._
import riff.raft.log.{LogCoords, LogEntry}
import riff.raft.messages._
import riff.raft.timer.LoggedInvocationTimer

class LeaderNodeTest extends RiffSpec {

  //
  //  "LeaderNode append response from an out-of-date node" should {
  //    "send a 'max batch size' append list to catch-up nodes" in {
  //      ???
  //    }
  //  }
  //
  //  "RaftNode onAppend" should {
  //    "not append entries from a request with an earlier term" in {
  //      ???
  //    }
  //  }
//  "LeaderNode updating its cluster view on heartbeat replies" should {
//
//  }
  "LeaderNode brings new client up-to-speed" should {
    // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
    Given("A cluster of nodes A, B and C")
    // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
    val cluster       = TestCluster(3)
    val List(a, b, c) = cluster.clusterNodes.ensuring(_.forall(_.persistentState.currentTerm == 0))

    // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
    And("A becomes the leader and replicated 10 append requests")
    // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
    cluster.sendMessages(a.nodeKey, cluster.electLeader(a).toList)
    val AddressedRequest(appendRequestsFromLeader) = a.createAppend((100 until 110).toArray)
    cluster.sendMessages(a.nodeKey, appendRequestsFromLeader.toList)
    cluster.clusterNodes.foreach(_.log.latestAppended() shouldBe LogCoords(1,10))

    // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
    And("The leader then accepts 90 more appends, which aren't sent to the followers")
    // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
    And("The leader then accepts 90 more appends, which aren't sent to the followers")
    // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
    When("A follower then accepts a heartbeat msg w/ a previous index as 100 from the leader")
    // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
    Then("the leader should continue to sent HB msgs to that follower until it reaches the 10th entry")
    // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

    // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
    Then("The leader should then send batches of updates based on its max batch size until the follower is caught up")
    // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =


  }
  "LeaderNode when disconnected from the rest of the cluster" should {

    "have its unreplicated entries overwritten by entries from a new leader" in {

      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      Given("A cluster of nodes A, B and C, where A is the leader")
      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      val cluster       = TestCluster(3)
      val List(a, b, c) = cluster.clusterNodes.ensuring(_.forall(_.persistentState.currentTerm == 0))
      cluster.clusterNodes.foreach(_.persistentState.currentTerm shouldBe 0)
      cluster.electLeader(a.nodeKey)
      cluster.clusterNodes.foreach(_.persistentState.currentTerm shouldBe 1)

      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      When("An entry is appended to the leader node, but not yet replicated")
      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      val firstLeaderLogEntryData                    = 123456
      val AddressedRequest(appendRequestsFromLeader) = a.createAppend(firstLeaderLogEntryData)

      withClue("the leader should try and send append requests to B and C (which we won't do in this test)") {
        appendRequestsFromLeader.map(_._1).toSet shouldBe Set("node 2", "node 3")
      }
      a.log.latestAppended() shouldBe LogCoords(1, 1)
      a.log.entryForIndex(1) shouldBe Some(LogEntry(1, firstLeaderLogEntryData))

      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      And("Follower node B requests a vote (a scenario where leader A becomes unresponsive)")
      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      val voteExchanges = cluster.attemptToElectLeader(b).partition {
        case (RequestVoteResponse(_, ok), _) => ok
        case other => fail(s"got $other")
      }

      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      Then("Then it should get a vote from C, but not A, and so become the leader")
      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      val (List((RequestVoteResponse(2, true), AddressedRequest(newLeaderHeartbeats: Iterable[(String, RaftRequest[LogIndex])]))),
           List((RequestVoteResponse(2, false), _))) = voteExchanges
      withClue("the leader A should've voted NO as it has a more complete log, but Follower C should grant the vote") {
        newLeaderHeartbeats.map(_._1).toSet shouldBe Set("node 1", "node 3")
        newLeaderHeartbeats.map(_._2).foreach {
          case AppendEntries(LogCoords.Empty, 2, 0, entries) => entries shouldBe empty
          case other => fail(s"got $other")
        }
      }

      a.raftNode().role shouldBe Follower
      b.raftNode().role shouldBe Leader
      c.raftNode().role shouldBe Follower
      cluster.clusterNodes.foreach(_.persistentState.currentTerm shouldBe 2)

      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      When("The new leader sends its expected heartbeat message")
      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      cluster.sendMessages(b.nodeKey, newLeaderHeartbeats).keySet shouldBe Set("node 1", "node 3")

      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      And("subsequently appends a new entry")
      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      val newLeaderFirstAppendData                  = 8888
      val AddressedRequest(newLeaderAppendRequests) = b.createAppend(newLeaderFirstAppendData)
      newLeaderAppendRequests.foreach {
        case (_, AppendEntries(LogCoords.Empty, 2, 0, data)) => data.toList shouldBe List(LogEntry(2, newLeaderFirstAppendData))
        case other => fail(s"got $other")
      }

      withClue("Before the old leader gets the append it should have the old values") {
        a.log.latestAppended() shouldBe LogCoords(1, 1)
        a.log.entryForIndex(1) shouldBe Some(LogEntry(1, firstLeaderLogEntryData))
      }

      val newAppendResponses = cluster.sendMessages(b.nodeKey, newLeaderAppendRequests)
      newAppendResponses shouldBe Map(
        "node 1" -> AddressedResponse("node 2", AppendEntriesResponse(2, true, 1)),
        "node 3" -> AddressedResponse("node 2", AppendEntriesResponse(2, true, 1))
      )

      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      Then("the older leader A, as well as the other nodes, should overwrite its unreplicated entry")
      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

      cluster.clusterNodes.map(_.log).foreach { log =>
        log.latestAppended() shouldBe LogCoords(2, 1)
        log.entryForIndex(1) shouldBe Some(LogEntry(2, newLeaderFirstAppendData))
      }
    }

  }

  "LeaderNode.onAppendResponse" should {
    for {
      clusterSize                    <- (1 to 10)
      numberOfAppendResponsesToApply <- (1 until clusterSize)
    } {
      val leaderExpectedToCommit = isMajority(numberOfAppendResponsesToApply + 1, clusterSize)

      val verb              = if (leaderExpectedToCommit) "commit" else "not commit"
      def nameForId(i: Int) = s"member$i"
      val peerNames         = (1 until clusterSize).map(nameForId).toSet

      s"$verb entries when ${numberOfAppendResponsesToApply}/${peerNames.size} peers in a cluster of $clusterSize nodes have the entry" in {

        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        Given(s"a cluster of size '$clusterSize' (${peerNames.size} followers + 1 leader)")
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        import RichNodeState._

        val leaderState = new LeaderNode("the leader", new LeaderState(peerNames.map(_ -> Peer()).toMap))

        val thisTerm = 2

        val leaderTimer = new LoggedInvocationTimer[String]
        val leader = {
          implicit val timer = leaderTimer
          NodeState.inMemory[String, String](leaderState.id).withRaftNode(leaderState).withTerm(thisTerm)
        }

        // just for fun, let's also create some instance to which we can apply the requests
        val peerInstancesByName: Map[String, NodeState[String, String]] = peerNames.map { name =>
          val peersToThisNode    = (peerNames - name) + leaderState.id
          implicit val peerTimer = new LoggedInvocationTimer[String]
          val peerState          = NodeState.inMemory[String, String](name).withCluster(RaftCluster(peersToThisNode))
          name -> peerState
        }.toMap

        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        When("the leader is asked to create some append entry requests")
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        val leaderLog = leader.log

        // the leader should always have a log which has committed its entry

        val AddressedRequest(appendRequestsFromLeader) = leader.createAppend(Array("foo", "bar", "bazz"))
        //val (leaderAppendResponse, requests) = leader.makeAppendEntries(leaderLog, 7, Array("foo", "bar", "bazz"))

        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        Then("the leader should reset its 'send heartbeat' timer for its peers")
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        val resetSendCalls: List[(String, Option[String])] = leaderTimer.resetSendHeartbeatCalls()
        resetSendCalls.size shouldBe leaderState.clusterSize - 1
        resetSendCalls should contain theSameElementsAs peerNames.map(_ -> None)

        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        Then("the leader log should immediately contain the (uncommitted) entries")
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        //leaderAppendResponse shouldBe LogAppendResult(term = thisTerm, firstIndex = 1, lastIndex = 3)
        leaderLog.latestAppended() shouldBe LogCoords(thisTerm, 3)
        leaderLog.latestCommit() shouldBe 0
        val leaderEntries = leaderLog.entriesFrom(1)
        leaderEntries should contain inOrderOnly (
          LogEntry(thisTerm, "foo"),
          LogEntry(thisTerm, "bar"),
          LogEntry(thisTerm, "bazz")
        )

        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        When(s"$numberOfAppendResponsesToApply of those requests are applied to our peers (which are leaderExpectedToCommit to append successfully)")
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        def applyAppendToPeer(peerName: String, appendRequest: AppendEntries[String]): (String, AppendEntriesResponse) = {

          appendRequest.previous shouldBe LogCoords.Empty
          appendRequest.term shouldBe thisTerm
          appendRequest.commitIndex shouldBe 0
          appendRequest.entries should contain allOf (LogEntry(thisTerm, "foo"), LogEntry(thisTerm, "bar"), LogEntry(thisTerm, "bazz"))

          val peerState = peerInstancesByName(peerName)

          val peerTimer   = peerState.timers.timer.asInstanceOf[LoggedInvocationTimer[String]]
          val callsBefore = peerTimer.resetReceiveHeartbeatCalls()
          callsBefore shouldBe (empty)

          // the peer should immediately update its term to the leaders term and reset its append heartbeat
          val AddressedResponse(from, resp: AppendEntriesResponse) = peerState.onMessage(leaderState.id, appendRequest)
          from shouldBe leaderState.id
          peerState.persistentState.currentTerm shouldBe thisTerm

          val callsAfter = peerTimer.resetReceiveHeartbeatCalls()
          callsAfter shouldBe List(peerName -> None)

          resp.matchIndex shouldBe 3
          resp.success shouldBe true
          resp.term shouldBe thisTerm
          peerName -> resp
        }

        val requestsToApply = appendRequestsFromLeader.take(numberOfAppendResponsesToApply)
        val responses: Iterable[(String, AppendEntriesResponse)] = requestsToApply.map {
          case (nodeName, req: AppendEntries[String]) => applyAppendToPeer(nodeName, req)
          case other                                  => fail(s"Expected an append entries request but got $other")
        }

        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        And(s"The leader gets the $numberOfAppendResponsesToApply responses applied")
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        val responseAfterApplingToPeer = responses.map {
          case (peerName, appendResponseFromPeer) =>
            withClue("The leader's known match state prior to hearing from the peer should be zero") {
              leaderState.clusterView.stateForPeer(peerName).get.matchIndex shouldBe 0
            }

            // update the leader w/ the peer's response
            val (committed, leaderReply) = leaderState.onAppendResponse(peerName, leaderLog, currentTerm = thisTerm, appendResponseFromPeer, maxAppendSize = 10)

            withClue("The leader should now have updated its view of the peer") {
              leaderState.clusterView.stateForPeer(peerName).get.matchIndex shouldBe appendResponseFromPeer.matchIndex
              leaderState.clusterView.stateForPeer(peerName).get.nextIndex shouldBe 4
            }

            leaderReply.isInstanceOf[NoOpOutput] shouldBe true

            (peerName, committed, leaderReply)
        }
        responseAfterApplingToPeer.size shouldBe numberOfAppendResponsesToApply

        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        val expectationVerb = if (leaderExpectedToCommit) "be" else "not be"
        Then(s"The leader's log entry should ${expectationVerb} committed")
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        if (leaderExpectedToCommit) {
          leaderLog.latestCommit() shouldBe 3
        } else {
          leaderLog.latestCommit() shouldBe 0
        }
      }
    }
  }

}
