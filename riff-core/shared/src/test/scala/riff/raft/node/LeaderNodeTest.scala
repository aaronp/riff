package riff.raft.node

import riff.RiffSpec
import riff.raft._
import riff.raft.log.{LogAppendResult, LogCoords, LogEntry, RaftLog}
import riff.raft.messages.{AppendEntries, _}
import riff.raft.node.NoOpResult.LogMessageResult
import riff.raft.timer.LoggedInvocationClock

class LeaderNodeTest extends RiffSpec {

  "LeaderNodeState.onAppendResponse with successful update" should {
    "send the next append entries when receiving a successful append response which is behind the leader's log" in {

      Given("A leader node w/ 100 log entries")
      val leader = new LeaderNodeState("the leader", LeadersClusterView("follower1", "follower2"))
      val log    = RaftLog.inMemory[String]()
      // in this case every log entry is for a different term, 100 + i
      val someData = (1 to 100).map(i => LogEntry(100 + i, i.toString)).toArray
      log.appendAll(1, someData) shouldBe LogAppendResult(LogCoords(101, 1), LogCoords(200, 100))
      log.latestAppended() shouldBe LogCoords(200, 100)

      When("The leader receives a successful AppendEntriesResponse w/ a match index 7 and a maxAppendSize 12")
      val appendResponse                                = AppendEntriesResponse.ok(1, 7)
      val (committedCoords, AddressedRequest(requests)) = leader.onAppendResponse("follower1", log, currentTerm = 500, appendResponse, maxAppendSize = 5)

      Then("It should update its cluster view")
      leader.clusterView.toMap() shouldBe Map("follower1" -> Peer.withMatchIndex(7), "follower2" -> Peer.Empty)

      Then("It should commit all values through the match index, as there is now a majority in a 3 node cluster")
      committedCoords.toList should contain theSameElementsInOrderAs ((1 to 7).map(i => LogCoords(100 + i, i)))
      log.latestCommit() shouldBe 7

      And("It should send another AppendEntries w/ 5 values, 8 to 13")
      val List(("follower1", nextAppendEntries)) = requests.toList

      log.coordsForIndex(8) shouldBe Some(LogCoords(108, 8))
      log.entryForIndex(8) shouldBe Some(LogEntry(108, "8"))

      nextAppendEntries shouldBe AppendEntries(LogCoords(107, 7),
                                               500,
                                               commitIndex = 7,
                                               Array(
                                                 LogEntry(108, "8"),
                                                 LogEntry(109, "9"),
                                                 LogEntry(110, "10"),
                                                 LogEntry(111, "11"),
                                                 LogEntry(112, "12")
                                               ))
    }
    "not send an AppendEntries request when it is up-to-date w/ the leader's log" in {

      Given("A leader node w/ 100 log entries")
      val leader = new LeaderNodeState("the leader", LeadersClusterView("follower1", "follower2"))
      val log    = RaftLog.inMemory[String]()
      // in this case every log entry is for a different term, 100 + i
      val someData = (1 to 100).map(i => LogEntry(100 + i, i.toString)).toArray
      log.appendAll(1, someData) shouldBe LogAppendResult(LogCoords(101, 1), LogCoords(200, 100))
      log.latestAppended() shouldBe LogCoords(200, 100)

      When("it receives an append response (heartbeat response) matching the leader's latest log entry")
      val (committedCoords, LogMessageResult(msg)) = leader.onAppendResponse("follower2", log, 6, AppendEntriesResponse.ok(term = 6, matchIndex = 100), 100)

      Then("it should commit the entries up to match index")
      committedCoords.size shouldBe 100
    }
    "commit the entries when a majority is reached" in {
      val leader = new LeaderNodeState("the leader", LeadersClusterView("follower1", "follower2"))

      val log                               = RaftLog.inMemory[String]()
      val someData: Array[LogEntry[String]] = (1 to 100).map(i => LogEntry(100 + i, i.toString)).toArray
      log.appendAll(1, someData) shouldBe LogAppendResult(LogCoords(101, 1), LogCoords(200, 100))
      log.latestAppended() shouldBe LogCoords(200, 100)

      val appendResponse                          = AppendEntriesResponse.ok(1, 7)
      val (committed, AddressedRequest(requests)) = leader.onAppendResponse("follower1", log, 123, appendResponse, maxAppendSize = 2)
      committed should contain theSameElementsInOrderAs (someData.take(7).map(e => LogCoords(e.term, e.data.toInt)))
      val List(("follower1", AppendEntries(LogCoords(107, 7), 123, 7, entries))) = requests.toList
      log.latestCommit() shouldBe appendResponse.matchIndex
      entries should contain only (
        LogEntry(108, "8"),
        LogEntry(109, "9")
      )
    }
  }

  "LeaderNodeState.onAppendResponse with failed update" should {
    "send a previous append entries for the previous index on failure" in {
      val leader =
        new LeaderNodeState("the leader", LeadersClusterView("follower1" -> Peer.withUnmatchedNextIndex(10), "follower2" -> Peer.withUnmatchedNextIndex(10)))
      leader.clusterView.stateForPeer("follower1") shouldBe Some(Peer.withUnmatchedNextIndex(10))
      leader.clusterView.stateForPeer("follower2") shouldBe Some(Peer.withUnmatchedNextIndex(10))
      leader.clusterView.stateForPeer("unknown") shouldBe None

      val appendResponse = AppendEntriesResponse.fail(1)

      val log      = RaftLog.inMemory[String]()
      val someData = (1 to 100).map(i => LogEntry(100 + i, i.toString)).toArray
      log.appendAll(1, someData) shouldBe LogAppendResult(LogCoords(101, 1), LogCoords(200, 100))
      log.latestAppended() shouldBe LogCoords(200, 100)

      val (Nil, AddressedRequest(requests)) = leader.onAppendResponse("follower1", log, 123, appendResponse, 10)
      val List(("follower1", nextAppend))   = requests.toList
      nextAppend shouldBe AppendEntries(LogCoords(109, 9), 123, 0)

      leader.clusterView.stateForPeer("follower1") shouldBe Some(Peer.withUnmatchedNextIndex(9))
      leader.clusterView.stateForPeer("follower2") shouldBe Some(Peer.withUnmatchedNextIndex(10))
    }
  }
  "LeaderNodeState send heartbeat timeout" should {
    "re-send missing AppendEntry requests when the send-heartbeat times out" in {
      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      Given("A cluster of nodes A, B and C")
      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      val cluster       = TestCluster(3)
      val List(a, b, c) = cluster.clusterNodes.ensuring(_.forall(_.persistentState.currentTerm == 0))

      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      And("A becomes the leader and replicated 10 append requests")
      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      cluster.sendMessages(a.nodeId, cluster.electLeader(a).toList)
      val AddressedRequest(appendRequestsFromLeader)                       = a.createAppend((100 until 110).toArray)
      val appendResponses: Map[String, RaftNode[LogIndex]#Result] = cluster.sendMessages(a.nodeId, appendRequestsFromLeader.toList)

      withClue("Before receiving the append responses the leader should assume a default view of the cluster") {
        val initialView: LeadersClusterView = a.state().asLeader.get.clusterView
        initialView.numberOfPeers shouldBe 2
        initialView.stateForPeer(b.nodeId) shouldBe Some(Peer.Empty)
        initialView.stateForPeer(c.nodeId) shouldBe Some(Peer.Empty)
      }

      cluster.sendResponses(appendResponses)

      withClue("After receiving the followers' AppendEntriesResponses it should update its view") {
        val afterFirst10AckView: LeadersClusterView = a.state().asLeader.get.clusterView
        afterFirst10AckView.numberOfPeers shouldBe 2
        afterFirst10AckView.stateForPeer(b.nodeId) shouldBe Some(Peer.withMatchIndex(10))
        afterFirst10AckView.stateForPeer(c.nodeId) shouldBe Some(Peer.withMatchIndex(10))
        a.log.latestCommit() shouldBe 10
        b.log.latestCommit() shouldBe 0
        c.log.latestCommit() shouldBe 0
      }
      cluster.clusterNodes.foreach(_.log.latestAppended() shouldBe LogCoords(1, 10))

      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      And("The leader then accepts 90 more appends, which aren't received by the followers")
      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      // we add these to the leader, but never send the requests to the followers
      a.createAppend((110 until 200).toArray)
      a.log.latestAppended() shouldBe LogCoords(1, 100)

      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      When("The leader next times out w/ for sending a HB msg to a follower")
      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      val AddressedRequest(heartbeatForB) = a.onTimerMessage(SendHeartbeatTimeout)
      heartbeatForB.toMap.keySet shouldBe Set(b.nodeId, c.nodeId)
      val AppendEntries(LogCoords(1, 10), 1, 10, entries) = heartbeatForB.toMap.apply(b.nodeId)
      entries.size shouldBe a.maxAppendSize

      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      Then(s"it should send an append msg which contains entries from 11 to ${11 + a.maxAppendSize}")
      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

      val bResp: Map[String, RaftNode[LogIndex]#Result] = cluster.sendMessages(a.nodeId, heartbeatForB.toList)
      b.log.entriesFrom(1).map(_.data).toList shouldBe (100 until 120).toList

      bResp.keySet shouldBe Set(b.nodeId, c.nodeId)
      bResp(b.nodeId) shouldBe AddressedResponse(a.nodeId, AppendEntriesResponse(1, true, 20))

      // update the leader w/ node 2's response
      a.state().asLeader.get.clusterView.toMap() shouldBe Map("node 2" -> Peer.withMatchIndex(10), "node 3" -> Peer.withMatchIndex(10))

      // just send the response from b
      cluster.sendResponses(bResp - c.nodeId)
      a.state().asLeader.get.clusterView.toMap() shouldBe Map("node 2" -> Peer.withMatchIndex(20), "node 3" -> Peer.withMatchIndex(10))
    }
  }

  "LeaderNodeState when disconnected from the rest of the cluster" should {

    "have its unreplicated entries overwritten by entries from a new leader" in {

      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      Given("A cluster of nodes A, B and C, where A is the leader")
      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      val cluster       = TestCluster(3)
      val List(a, b, c) = cluster.clusterNodes.ensuring(_.forall(_.persistentState.currentTerm == 0))
      cluster.clusterNodes.foreach(_.persistentState.currentTerm shouldBe 0)
      cluster.electLeader(a.nodeId)
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
        case other                           => fail(s"got $other")
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
          case other                                         => fail(s"got $other")
        }
      }

      a.state().role shouldBe Follower
      b.state().role shouldBe Leader
      c.state().role shouldBe Follower
      cluster.clusterNodes.foreach(_.persistentState.currentTerm shouldBe 2)

      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      When("The new leader sends its expected heartbeat message")
      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      cluster.sendMessages(b.nodeId, newLeaderHeartbeats).keySet shouldBe Set("node 1", "node 3")

      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      And("subsequently appends a new entry")
      // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
      val newLeaderFirstAppendData                  = 8888
      val AddressedRequest(newLeaderAppendRequests) = b.createAppend(newLeaderFirstAppendData)
      newLeaderAppendRequests.foreach {
        case (_, AppendEntries(LogCoords.Empty, 2, 0, data)) => data.toList shouldBe List(LogEntry(2, newLeaderFirstAppendData))
        case other                                           => fail(s"got $other")
      }

      withClue("Before the old leader gets the append it should have the old values") {
        a.log.latestAppended() shouldBe LogCoords(1, 1)
        a.log.entryForIndex(1) shouldBe Some(LogEntry(1, firstLeaderLogEntryData))
      }

      val newAppendResponses = cluster.sendMessages(b.nodeId, newLeaderAppendRequests)
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

  "LeaderNodeState.onAppendResponse" should {
    for {
      clusterSize                    <- (1 to 10)
      numberOfAppendResponsesToApply <- (1 until clusterSize)
    } {
      val leaderExpectedToCommit = isMajority(numberOfAppendResponsesToApply + 1, clusterSize)

      val verb                   = if (leaderExpectedToCommit) "commit" else "not commit"
      def nameForId(i: Int)      = s"member$i"
      val peerNames: Set[String] = (1 until clusterSize).map(nameForId).toSet

      s"$verb entries when ${numberOfAppendResponsesToApply}/${peerNames.size} peers in a cluster of $clusterSize nodes have the entry" in {

        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        Given(s"a cluster of size '$clusterSize' (${peerNames.size} followers + 1 leader)")
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        import RichNodeState._

        val leaderState = new LeaderNodeState("the leader", LeadersClusterView(peerNames.toList: _*))

        val thisTerm = 2

        val leaderTimer = new LoggedInvocationClock
        val leader = {
          implicit val timer = leaderTimer
          RaftNode.inMemory[String](leaderState.id).withRaftNode(leaderState).withTerm(thisTerm)
        }

        // just for fun, let's also create some instance to which we can apply the requests
        val peerInstancesByName: Map[String, RaftNode[String]] = peerNames.map { name =>
          val peersToThisNode    = (peerNames - name) + leaderState.id
          implicit val peerTimer = new LoggedInvocationClock
          val peerState          = RaftNode.inMemory[String](name).withCluster(RaftCluster(peersToThisNode)).withTerm(thisTerm)
          name -> peerState
        }.toMap

        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        When("the leader is asked to create some append entry requests")
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        val leaderLog = leader.log

        // the leader should always have a log which has committed its entry

        val AddressedRequest(appendRequestsFromLeader) = leader.createAppend(Array("foo", "bar", "bazz"))

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

          val peerState: RaftNode[String] = peerInstancesByName(peerName)

          val peerTimer   = peerState.timers.clock.asInstanceOf[LoggedInvocationClock]
          val callsBefore = peerTimer.resetReceiveHeartbeatCalls()
          callsBefore shouldBe 0

          // the peer should immediately update its term to the leaders term and reset its append heartbeat
          val AddressedResponse(from, resp: AppendEntriesResponse) = peerState.handleMessage(leaderState.id, appendRequest)
          from shouldBe leaderState.id
          peerState.persistentState.currentTerm shouldBe thisTerm

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

            leaderReply.isInstanceOf[NoOpResult] shouldBe true

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
