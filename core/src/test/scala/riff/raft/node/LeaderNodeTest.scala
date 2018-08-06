package riff.raft.node
import riff.RiffSpec
import riff.raft._
import riff.raft.log.{LogCoords, LogEntry}
import riff.raft.messages.{AppendEntries, AppendEntriesResponse}
import riff.raft.timer.LoggedInvocationTimer

class LeaderNodeTest extends RiffSpec {

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

        val thisTerm = 7

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
