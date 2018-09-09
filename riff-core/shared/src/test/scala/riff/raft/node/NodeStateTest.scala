package riff.raft.node
import riff.RiffSpec
import riff.raft.log.LogCoords
import riff.raft.messages.{ReceiveHeartbeatTimeout, RequestVote, RequestVoteResponse}

class NodeStateTest extends RiffSpec {
  import RichNodeState._
  "NodeState.onTimerMessage" should {
    "become a leader if it is a cluster of 1" in {
      val node = newNode()
      node.raftNode().isFollower shouldBe true
      node.persistentState.currentTerm shouldBe 0

      // call the method under test
      node.onTimerMessage(ReceiveHeartbeatTimeout)

      node.raftNode().isLeader shouldBe true
      node.persistentState.currentTerm shouldBe 1
    }
    "become a candidate in a cluster of 2 when it receives a receive heartbeat timeout as a follower" in {
      val node: NodeState[String, Int] = newNode().withCluster(RaftCluster("neighbor"))
      node.raftNode().isFollower shouldBe true
      node.persistentState.currentTerm shouldBe 0

      // call the method under test
      node.onTimerMessage(ReceiveHeartbeatTimeout)

      node.raftNode().isCandidate shouldBe true
      node.persistentState.currentTerm shouldBe 1
    }
    "become a candidate in a cluster of 2 with a new term when it receives a receive heartbeat timeout as a candidate" in {
      val node = newNode().withCluster(RaftCluster("neighbor"))
      node.raftNode().isFollower shouldBe true
      node.persistentState.currentTerm shouldBe 0

      node.onTimerMessage(ReceiveHeartbeatTimeout)
      node.persistentState.currentTerm shouldBe 1
      node.raftNode().isCandidate shouldBe true

      node.onTimerMessage(ReceiveHeartbeatTimeout)
      node.persistentState.currentTerm shouldBe 2
      node.raftNode().isCandidate shouldBe true
    }
    "become a candidate in cluster of 2 with a new term when it receives a receive heartbeat timeout as a leader" in {
      import RichNodeState._
      val node = newNode().withRaftNode(new LeaderNode("the leader", ClusterView(0)))
      node.raftNode().isLeader shouldBe true
      node.persistentState.currentTerm shouldBe 0

      node.onTimerMessage(ReceiveHeartbeatTimeout)
      node.persistentState.currentTerm shouldBe 1
      node.raftNode().isCandidate shouldBe true
    }
  }
  "NodeState onRequestVote" should {
    "not grant a vote if it is for an earlier term" in {
      val ourTerm = 2
      val node    = newNode().withLog(logWithCoords(LogCoords(2, 0))).withTerm(ourTerm)

      val reply = node.onRequestVote("someone", RequestVote(term = ourTerm - 1, logState = LogCoords.Empty))
      reply shouldBe RequestVoteResponse(ourTerm, false)
    }
  }
}