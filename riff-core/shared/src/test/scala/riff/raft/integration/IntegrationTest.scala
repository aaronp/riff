package riff.raft.integration

import riff.RiffSpec
import riff.raft.integration.simulator._
import riff.raft.log.{LogCoords, LogEntry}
import riff.raft.messages.AppendEntries
import riff.raft.node.{Follower, Leader}

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

      simulator.leader() shouldBe empty
      withClue("the new node should've set a receive HB timeout on creation") {
        simulator.timelineValues should contain only (ReceiveTimeout(nameForIdx(1)))
      }

      val result = simulator.advance()
      result.afterState(1).role shouldBe Leader
      result.leader.persistentStateSnapshot.currentTerm shouldBe 1

      simulator.leader() should not be (empty)
      simulator.timelineValues should contain only (SendTimeout(nameForIdx(1)))
    }
    "bring disconnected back up-to-date" in {
      Given("A cluster of four nodes w/ an elected leader")
      val simulator: RaftSimulator = RaftSimulator.clusterOfSize(4)

      val result = simulator.advanceUntil(_.hasLeader)

      When("A follower is removed")
      val someFollower: NodeSnapshot[String] = result.nodesWithRole(Follower).head
      simulator.killNode(someFollower.name)

      And("The remaining nodes have some entries replicated")
      (0 to 5).foreach { i => simulator.appendToLeader(Array(s"some entry $i"))
      }
      simulator.advanceUntil(_.leader.log.latestCommit == 5)

      When("A follower times out and becomes leader")
      val newLeader = simulator.nodesWithRole(Follower).head
      newLeader.onReceiveHeartbeatTimeout()

      try {
        simulator.advanceUntil(r => r.leader.name == newLeader.nodeKey)
      } catch {
        case bang : Exception =>
          println(bang)

          val timeline = simulator.currentTimeline()
          println(simulator)

          val fresh: RaftSimulator = RaftSimulator.clusterOfSize(4)
          val results              = fresh.replay(timeline)

          println(results)
      }
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
