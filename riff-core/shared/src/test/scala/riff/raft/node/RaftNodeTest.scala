package riff.raft.node
import riff.RiffSpec
import riff.raft.log.{LogCoords, LogEntry}
import riff.raft.messages._

class RaftNodeTest extends RiffSpec {
  import RichNodeState._

  "RaftNode.createAppend" should {

    "commit the entry immediately if it is a single node cluster" in {
      val node = newNode()
      node.onTimerMessage(ReceiveHeartbeatTimeout)
      node.state().isLeader shouldBe true

      node.createAppend(Array(123))
      node.log.latestAppended() shouldBe LogCoords(1, 1)
      node.log.latestCommit() shouldBe 1
    }
    "not commit the entry it has a single peer" in {
      val node = newNode().withCluster(RaftCluster("single peer"))
      node.onTimerMessage(ReceiveHeartbeatTimeout)
      node.onRequestVoteResponse("single peer", RequestVoteResponse(1, true))
      node.state().isLeader shouldBe true

      node.createAppend(Array(123))
      node.log.latestAppended() shouldBe LogCoords(1, 1)
      node.log.latestCommit() shouldBe 0
    }
  }
  "RaftNode.onAppendResponse" should {
    "commit entries up-to the latest committed from the leader" in {

      // ---------------------------------------------------------------------------------------
      Given("A node with some initial log entries and which has a state-machine log")
      val ourTerm = 2

      // the log actually has two actions, just to prove each gets invoked
      var firstCommittedList   = List[LogEntry[Int]]()
      var anotherCommittedList = List[LogEntry[Int]]()
      val stateMachineLog = {
        logWithCoords(LogCoords(2, 4))
          .onCommit { x => firstCommittedList = firstCommittedList :+ x
          }
          .onCommit { x => anotherCommittedList = anotherCommittedList :+ x
          }
      }
      val node: RaftNode[String, Int] = newNode().withLog(stateMachineLog).withTerm(ourTerm)
      node.state().role shouldBe Follower

      // ---------------------------------------------------------------------------------------
      When("The node receives an AppendEntries request which commits the first entry")
      val appendWithCommit = AppendEntries(LogCoords(2, 4), term = 2, commitIndex = 1, Array(LogEntry(2, 1234)))
      node.onAppendEntries("the leader", appendWithCommit) shouldBe AppendEntriesResponse.ok(term = 2, matchIndex = 5)

      // ---------------------------------------------------------------------------------------
      Then("The node should commit its entry")
      firstCommittedList shouldBe List(LogEntry(2, 1000))
      anotherCommittedList shouldBe firstCommittedList
      node.log.latestAppended() shouldBe LogCoords(2, 5)

      // ---------------------------------------------------------------------------------------
      When("It receives another append but w/ the same commit index")
      val appendWithCommit2 = AppendEntries(LogCoords(2, 5), term = 3, commitIndex = 1, Array(LogEntry(3, 5678)))
      node.onAppendEntries("a new leader", appendWithCommit2) shouldBe AppendEntriesResponse.ok(term = 3, matchIndex = 6)

      // ---------------------------------------------------------------------------------------
      Then("The log should have the new entry, but not have tried to commit anything new")
      firstCommittedList shouldBe List(LogEntry(2, 1000))
      anotherCommittedList shouldBe firstCommittedList
      node.log.latestAppended() shouldBe LogCoords(3, 6)
      node.persistentState.currentTerm shouldBe 3

      // ---------------------------------------------------------------------------------------
      When("It receives yes another append w/ no data but 6 commits")
      val ae = AppendEntries[Int](LogCoords(3, 6), term = 3, commitIndex = 5)
      val actual = node.onAppendEntries("a new leader", ae)

      actual shouldBe AppendEntriesResponse.ok(term = 3, matchIndex = 6)

      // ---------------------------------------------------------------------------------------
      Then("The log should've appended 3 entries")
      firstCommittedList shouldBe List(LogEntry(2, 1000), LogEntry(2, 1001), LogEntry(2, 1002), LogEntry(2, 1003), LogEntry(2, 1234))
      anotherCommittedList shouldBe firstCommittedList
      node.log.latestAppended() shouldBe LogCoords(3, 6)

    }
  }
  "RaftNode.onTimerMessage" should {
    "become a leader if it is a cluster of 1" in {
      val node = newNode()
      node.state().isFollower shouldBe true
      node.persistentState.currentTerm shouldBe 0

      // call the method under test
      node.onTimerMessage(ReceiveHeartbeatTimeout)

      node.state().isLeader shouldBe true
      node.persistentState.currentTerm shouldBe 1
    }
    "become a candidate in a cluster of 2 when it receives a receive heartbeat timeout as a follower" in {
      val node: RaftNode[String, Int] = newNode().withCluster(RaftCluster("neighbor"))
      node.state().isFollower shouldBe true
      node.persistentState.currentTerm shouldBe 0

      // call the method under test
      node.onTimerMessage(ReceiveHeartbeatTimeout)

      node.state().isCandidate shouldBe true
      node.persistentState.currentTerm shouldBe 1
    }
    "become a candidate in a cluster of 2 with a new term when it receives a receive heartbeat timeout as a candidate" in {
      val node = newNode().withCluster(RaftCluster("neighbor"))
      node.state().isFollower shouldBe true
      node.persistentState.currentTerm shouldBe 0

      node.onTimerMessage(ReceiveHeartbeatTimeout)
      node.persistentState.currentTerm shouldBe 1
      node.state().isCandidate shouldBe true

      node.onTimerMessage(ReceiveHeartbeatTimeout)
      node.persistentState.currentTerm shouldBe 2
      node.state().isCandidate shouldBe true
    }
    "become a candidate in cluster of 2 with a new term when it receives a receive heartbeat timeout as a leader" in {
      import RichNodeState._
      val cluster = RaftCluster("A follower")
      val node    = newNode().withCluster(cluster).withRaftNode(new LeaderNodeState("the leader", LeadersClusterView(cluster)))
      node.state().isLeader shouldBe true
      node.persistentState.currentTerm shouldBe 0

      node.onTimerMessage(ReceiveHeartbeatTimeout)
      node.persistentState.currentTerm shouldBe 1
      node.state().isCandidate shouldBe true
    }
  }
  "RaftNode onRequestVote" should {
    "not grant a vote if it is for an earlier term" in {
      val ourTerm = 2
      val node    = newNode().withLog(logWithCoords(LogCoords(2, 0))).withTerm(ourTerm)

      val reply = node.onRequestVote("someone", RequestVote(term = ourTerm - 1, logState = LogCoords.Empty))
      reply shouldBe RequestVoteResponse(ourTerm, false)
    }
  }
}
