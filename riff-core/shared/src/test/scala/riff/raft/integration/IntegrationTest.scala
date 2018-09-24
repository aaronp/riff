package riff.raft.integration

import riff.RiffSpec
import riff.raft.integration.simulator._
import riff.raft.log.{LogCoords, LogEntry}
import riff.raft.messages.AppendEntries
import riff.raft.node.{Follower, Leader, Peer}

/**
  * The other tests would create some inputs, feed them into the relevant functions/classes under test, and assert
  * some results.
  *
  * These tests rely on setting up some initial cluster and then just driving everything via 'append' requests on whatever
  * the current leader is.
  *
  * They use a [[RaftSimulator]], which is a test harness similar to what a 'real' setup would be (e.g. as we put the RaftNodes
  * behind an akka actor, monix stream, http service, etc).
  *
  */
class IntegrationTest extends RiffSpec {
  "Raft Cluster" should {
    "elect a leader event with a cluster of 1" in {
      val simulator = RaftSimulator.clusterOfSize(1)
      simulator.snapshotFor(1).persistentStateSnapshot.currentTerm shouldBe 0

      simulator.leaderStateOpt() shouldBe empty
      withClue("the new node should've set a receive HB timeout on creation") {
        simulator.timelineValues should contain only (ReceiveTimeout(nameForIdx(1)))
      }

      val result = simulator.advance()
      result.afterState(1).role shouldBe Leader
      result.leader.persistentStateSnapshot.currentTerm shouldBe 1

      simulator.leaderStateOpt() should not be (empty)
      simulator.timelineValues should contain only (SendTimeout(nameForIdx(1)))
    }

    "bring disconnected back up-to-date after a new leader election" in {
      Given("A cluster of four nodes w/ an elected leader")
      val simulator: RaftSimulator = RaftSimulator.clusterOfSize(4)

      simulator.advanceUntil(_.hasLeader)

      // format: off
      simulator.nodes().map { n => n.nodeKey -> n.state().role }.toMap shouldBe Map(
        "Node 1" -> Leader,
        "Node 2" -> Follower,
        "Node 3" -> Follower,
        "Node 4" -> Follower)
      // format: on

      When("A follower is removed")
      val someFollower = simulator.nodeFor(2)
      simulator.killNode(someFollower.state.id)

      And("The followers have not yet sent their AppendEntries responses")

      withClue(simulator.timelineAsExpectation()) {
        simulator.timelineAssertions shouldBe List(
          "SendRequest(Node 1, Node 2, AppendEntries(previous=LogCoords(0, 0), term=1, commit=0, []))",
          "SendResponse(Node 4, Node 1, RequestVoteResponse(term=1, granted=true))",
          "SendRequest(Node 1, Node 3, AppendEntries(previous=LogCoords(0, 0), term=1, commit=0, []))",
          "SendRequest(Node 1, Node 4, AppendEntries(previous=LogCoords(0, 0), term=1, commit=0, []))",
          "SendTimeout(Node 1)",
          "ReceiveTimeout(Node 3)",
          "ReceiveTimeout(Node 2)",
          "ReceiveTimeout(Node 4)"
        )
      }

      val view = simulator.leaderState.clusterView
      view.toMap shouldBe Map("Node 2" -> Peer.Empty, "Node 3" -> Peer.Empty, "Node 4" -> Peer.Empty)

      And("The remaining nodes have some entries replicated")

      // here we send five separate requests, as opposed to one request w/ five entries.
      // that means that the leader should immediately send the first request, but subsequent requests
      // won't match the leader's view of its peers, so the data (append requests) won't be sent until
      // the nodes response, thus allowing the leader to update its cluster view
      val data = (0 to 5).map { i =>
        // format: off
        val entry = s"some entry $i"
        simulator.appendToLeader(Array(entry))
        entry
        // format: on
      }

      Then("At this point the leader (with a log w/ a single entry, so the previous coords is still 0) has received some append requests")
      simulator.advanceUntil(_.leader.log.latestCommit == data.size)

      And("The leader and remaining followers should eventually sync their logs")
      simulator.advanceUntil { res =>
        val latestCommitByNodeName = res.nodeSnapshots.map { snap => snap.name -> snap.log.latestCommit
        }

        val nrCommitted = data.size
        latestCommitByNodeName.toMap == Map("Node 1" -> nrCommitted, "Node 2" -> 0, "Node 3" -> nrCommitted, "Node 4" -> nrCommitted)
      }

      When("A follower times out and becomes leader")
      val newLeader = nameForIdx(4)

      // force a timeout
      simulator.applyTimelineEvent(ReceiveTimeout(newLeader))
      simulator.advanceUntil(_.leaderOpt.exists(_.name == newLeader))

      And("The stopped node is restarted")
      simulator.restartNode(someFollower.state.id)


      Then("The new leader should bring the restarted node's log up-to-date")
      simulator.advanceUntil { res =>
        res.nodeSnapshots.forall(_.log.latestCommit == 6)
      }

      And("All the nodes logs should be equal")
      simulator.nodes().foreach(_.persistentState.currentTerm shouldBe 2)
      val logs = simulator.takeSnapshot().values.map(_.log)
      logs.foreach(_ shouldBe logs.head)
    }
    "dynamically add a node" in {
      Given("An initially empty cluster which elects itself as leader")
      val simulator = RaftSimulator.clusterOfSize(1)

      // timeout and elect ourselves
      simulator.advance().leader.persistentStateSnapshot.currentTerm shouldBe 1
      simulator.currentLeader().cluster.peers.size shouldBe 0

      When("An 'add node' entry is added to the leader's log")
      // create a log entry to add a cluster node
      val addNodeCommand = simulator.addNodeCommand()
      simulator.appendToLeader(Array(addNodeCommand))

      Then("The leader should immediately add the node to its cluster view")
      withClue(simulator.pretty()) {
        simulator.timelineValues() should contain allOf (ReceiveTimeout(nameForIdx(2)), SendTimeout(nameForIdx(1)))
      }

      simulator.currentLeader().cluster.peers.size shouldBe 1

      And(
        "Send a heartbeat to the new node when its 'send heartbeat' timer times out, resulting in the new node incrementing its term and resetting its receive timeout")

      val resultAfterLeaderSendHeartbeatTimeout = simulator.advance()

      resultAfterLeaderSendHeartbeatTimeout.afterState(1).cluster.peers should contain only (nameForIdx(2))
      resultAfterLeaderSendHeartbeatTimeout.afterState(2).cluster.peers should contain only (nameForIdx(1))

      withClue(resultAfterLeaderSendHeartbeatTimeout.toString) {
        resultAfterLeaderSendHeartbeatTimeout.timelineValues() should contain allOf (ReceiveTimeout(nameForIdx(2)), SendTimeout(nameForIdx(1)))
        val (_, firstHeartbeat) = resultAfterLeaderSendHeartbeatTimeout.afterTimeline.findOnly[SendRequest]
        firstHeartbeat.from shouldBe nameForIdx(1)
        firstHeartbeat.to shouldBe nameForIdx(2)
        val AppendEntries(LogCoords.Empty, 1, 1, addNodeCmd) = firstHeartbeat.request
        addNodeCmd should contain only LogEntry(1, addNodeCommand)
      }

      And("the send/receive heartbeats should've been reset")
      withClue(resultAfterLeaderSendHeartbeatTimeout.toString) {
        val b4Send    = resultAfterLeaderSendHeartbeatTimeout.beforeTimeline.findOnly[SendTimeout]
        val afterSend = resultAfterLeaderSendHeartbeatTimeout.afterTimeline.findOnly[SendTimeout]
        b4Send._1 should be < afterSend._1
      }

      When("the new follower receives the heartbeat")
      val resultAfterFollowerReceivingHeartbeat = simulator.advance()

      Then("it should reset its receive HB timeout")
      // the receive timeout should be unchanged until the follower receives the timeout
      val b4Receive    = resultAfterFollowerReceivingHeartbeat.beforeTimeline.findOnly[ReceiveTimeout]
      val afterReceive = resultAfterFollowerReceivingHeartbeat.afterTimeline.findOnly[ReceiveTimeout]
      b4Receive._1 should be < afterReceive._1
    }
  }
}
